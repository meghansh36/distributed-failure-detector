import asyncio
from contextlib import asynccontextmanager, closing

from protocol import AwesomeProtocol
from worker import Worker

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
