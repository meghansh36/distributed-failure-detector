import asyncio
from asyncio import DatagramProtocol, DatagramTransport, BaseTransport, Condition
from datetime import datetime
import logging
from typing import Optional, cast, Tuple
from collections import deque

class AwesomeProtocol(DatagramProtocol):

    def __init__(self) -> None:
        super().__init__()
        self._queue_lock = Condition()
        self._queue = deque()
        self._transport = None
    
    @property
    def transport(self) -> DatagramTransport:
        transport = self._transport
        assert transport is not None
        return transport

    def connection_made(self, transport: BaseTransport) -> None:
        self._transport = cast(DatagramTransport, transport)

    def datagram_received(self, data: bytes, addr: Tuple[str, int]) -> None:
        # parse data into a known structure
        # print(f"{datetime.now()}: received data: from {addr[0]}:{addr[1]}")
        asyncio.create_task(self._push((data, addr[0], addr[1])))
    
    def error_received(self, exc: Exception) -> None:
        logging.error('UDP operation failed')

    def connection_lost(self, exc: Optional[Exception]) -> None:
        logging.error(f'UDP connection lost: {exc}')
        self._transport = None

    async def _push(self, data) -> None:
        async with self._queue_lock:
            self._queue.append(data)
            self._queue_lock.notify()

    async def recv(self):
        async with self._queue_lock:
            await self._queue_lock.wait()
            return self._queue.popleft()

    async def send(self, host, port, data) -> None:
        self.transport.sendto(data, (host, port))
