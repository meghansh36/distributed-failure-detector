import asyncio
from asyncio import Event
from contextlib import suppress
from datetime import datetime
from weakref import WeakSet, WeakKeyDictionary
from typing import final, Final, NoReturn
from contextlib import asynccontextmanager, closing, suppress
from members import Member

from protocol import AwesomeProtocol


class UdpTransport():

    def __init__(self, host, port, members) -> None:
        self.host = host
        self.port = port
        self.members = members

    @property
    def bind_host(self) -> str:
        bind_host = self.host
        return bind_host

    @property
    def bind_port(self) -> int:
        bind_port = self.port
        return bind_port

    @asynccontextmanager
    async def enter(self):
        loop = asyncio.get_running_loop()
        transport, protocol = await loop.create_datagram_endpoint(
            lambda: AwesomeProtocol(),
            reuse_port=True, local_addr=(self.bind_host, self.bind_port))
        assert isinstance(protocol, AwesomeProtocol)
        worker = Worker(protocol, self.members)
        with closing(transport):
            yield worker


class Worker:

    def __init__(self, io: AwesomeProtocol, members) -> None:
        self.io: Final = io
        self._waiting: WeakKeyDictionary[Member, WeakSet[Event]] = WeakKeyDictionary()
        self._listening: WeakKeyDictionary[Member, WeakSet[Member]] = WeakKeyDictionary()
        self.members = members

    def _add_waiting(self, member, event: Event) -> None:
        waiting = self._waiting.get(member)
        if waiting is None:
            self._waiting[member] = waiting = WeakSet()
        waiting.add(event)

    def _add_listening(self, member, target) -> None:
        listening = self._listening.get(target)
        if listening is None:
            self._listening[target] = listening = WeakSet()
        listening.add(member)

    def _notify_waiting(self, member) -> None:
        waiting = self._waiting.get(member)
        if waiting is not None:
            for event in waiting:
                event.set()

    def _get_listening(self, member):
        listening = self._listening.pop(member, None)
        if listening is not None:
            return list(listening)
        else:
            return []

    async def _run_handler(self) -> NoReturn:
        while True:
            data, host, port = await self.io.recv()
            print(f'got this data: {data} from {host}:{port}')

            if (data == b"ack"):
                print(f'got ack from {host}:{port}')
                curr_member = None
                for member in self.members:
                    if port == member.port:
                        curr_member = member
                if member is not None:
                    self._notify_waiting(curr_member)
            else:
                await self.io.send(host, port, b"echo:" + data)

    async def _wait(self, target, timeout: float) -> bool:
        event = Event()
        self._add_waiting(target, event)
        with suppress(TimeoutError):
            await asyncio.wait_for(event.wait(), timeout)
        
        print('comming here...')
        return event.is_set()

    async def check(self, member: Member):
        print(f'sending pings to {member.host}:{member.port}')
        await self.io.send(member.host, member.port, b"ping")
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
