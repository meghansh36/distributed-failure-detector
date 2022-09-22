import asyncio
from asyncio import Event
from contextlib import suppress
from datetime import datetime
from weakref import WeakSet, WeakKeyDictionary
from typing import final, Final, NoReturn
from contextlib import suppress
from members import Member
from packets import Packet
# from pack_util import packet_pack, packet_unpack

from protocol import AwesomeProtocol

class Worker:

    def __init__(self, io: AwesomeProtocol, members) -> None:
        self.io: Final = io
        self._waiting: WeakKeyDictionary[Member, WeakSet[Event]] = WeakKeyDictionary()
        self.members = members

    def _add_waiting(self, member, event: Event) -> None:
        waiting = self._waiting.get(member)
        if waiting is None:
            self._waiting[member] = waiting = WeakSet()
        waiting.add(event)

    def _notify_waiting(self, member) -> None:
        waiting = self._waiting.get(member)
        if waiting is not None:
            for event in waiting:
                event.set()

    async def _run_handler(self) -> NoReturn:
        while True:
            packedPacket, host, port = await self.io.recv()
            receivedPacket = Packet.packet_unpack(packedPacket)
            print(f'got this data: {receivedPacket.data} from {host}:{port}')

            if (receivedPacket.packetType == "ACK"):
                print(f'got ack from {host}:{port}')
                curr_member = None
                for member in self.members:
                    if port == member.port:
                        curr_member = member
                if member is not None:
                    self._notify_waiting(curr_member)
            elif receivedPacket.packetType == "PING":
                ackPacket = Packet("ACK", "membership list")
                packedPacket = ackPacket.packet_pack()
                await self.io.send(host, port, packedPacket)

    async def _wait(self, target, timeout: float) -> bool:
        event = Event()
        self._add_waiting(target, event)
        with suppress(TimeoutError):
            await asyncio.wait_for(event.wait(), timeout)
        
        print('comming here...')
        return event.is_set()

    async def check(self, member: Member):
        print(f'sending pings to {member.host}:{member.port}')
        pingPacket = Packet("PING", "membership list")
        packedPacket = pingPacket.packet_pack()

        await self.io.send(member.host, member.port, packedPacket)

        online = await self._wait(member, 2)
        print(f'host online flag = {online}')

    async def run_failure_detection(self) -> NoReturn:
        while True:
            for member in self.members:
                asyncio.create_task(self.check(member))

            print(f'running failure detector: {datetime.now()}')
            await asyncio.sleep(20)

    @final
    async def run(self) -> NoReturn:
        await asyncio.gather(
            self._run_handler(),
            self.run_failure_detection())
        raise RuntimeError()
