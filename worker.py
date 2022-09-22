import asyncio
from asyncio import Event, exceptions
from datetime import datetime
from weakref import WeakSet, WeakKeyDictionary
from typing import final, Final, NoReturn

from config import Config, PING_TIMEOOUT, PING_DURATION
from members import Member
from packets import Packet, PacketType
from protocol import AwesomeProtocol

class Worker:

    def __init__(self, io: AwesomeProtocol, members) -> None:
        self.io: Final = io
        self._waiting: WeakKeyDictionary[Member, WeakSet[Event]] = WeakKeyDictionary()
        self.nodes = members

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
            
            packet = Packet.unpack(packedPacket)
            if not packet:
                continue
            
            print(f'got this data: {packet.data} from {host}:{port}')
            if packet.type == PacketType.ACK:
                print(f'got ack from {host}:{port}')
                curr_member = Config.get_member(hostname=host, port=port)
                if curr_member:
                    self._notify_waiting(curr_member)

            elif packet.type == PacketType.PING:
                # send ACK back to the node
                await self.io.send(host, port, Packet(PacketType.ACK, {}).pack())

    async def _wait(self, target: Member, timeout: float) -> bool:
        event = Event()
        self._add_waiting(target, event)

        try:
            await asyncio.wait_for(event.wait(), timeout)
        except exceptions.TimeoutError:
            print(f'failed to recieve ACK from {target.host}:{target.port}')
        except Exception as e:
            print(f'Exception when waiting for ACK from {target.host}:{target.port}: {e}')
    
        return event.is_set()

    async def check(self, member: Member):
        print(f'sending ping to {member.host}:{member.port}')
        await self.io.send(member.host, member.port, Packet(PacketType.PING, {}).pack())
        online = await self._wait(member, PING_TIMEOOUT)

    async def run_failure_detection(self) -> NoReturn:
        while True:
            for node in self.nodes:
                asyncio.create_task(self.check(node))

            print(f'running failure detector: {datetime.now()}')
            await asyncio.sleep(PING_DURATION)

    @final
    async def run(self) -> NoReturn:
        await asyncio.gather(
            self._run_handler(),
            self.run_failure_detection())
        raise RuntimeError()
