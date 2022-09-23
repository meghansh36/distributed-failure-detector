import asyncio
from asyncio import Event, exceptions
from datetime import datetime
from weakref import WeakSet, WeakKeyDictionary
from typing import final, Final, NoReturn, Optional
from config import Config, PING_TIMEOOUT, PING_DURATION
from nodes import Node
from packets import Packet, PacketType
from protocol import AwesomeProtocol
from membershipList import MemberShipList

class Worker:

    def __init__(self, io: AwesomeProtocol, nodes) -> None:
        self.io: Final = io
        self._waiting: WeakKeyDictionary[Node, WeakSet[Event]] = WeakKeyDictionary()
        self.nodes = nodes
        self.config: Config = None
        self.membership_list: Optional[MemberShipList] = None
    
    def initialize(self, config):
        self.config = config
        self.membership_list = MemberShipList(self.config.node)

    def _add_waiting(self, node: Node, event: Event) -> None:
        waiting = self._waiting.get(node)
        if waiting is None:
            self._waiting[node] = waiting = WeakSet()
        waiting.add(event)

    def _notify_waiting(self, node) -> None:
        waiting = self._waiting.get(node)
        if waiting is not None:
            for event in waiting:
                event.set()

    async def _run_handler(self) -> NoReturn:
        while True:
            
            packedPacket, host, port = await self.io.recv()
            
            packet: Packet = Packet.unpack(packedPacket)
            if not packet:
                continue
            
            # print(f'got this data: {packet.data} from {host}:{port}')

            if packet.type == PacketType.ACK:
                # print(f'got ack from {host}:{port}')
                curr_node = Config.get_node(hostname=host, port=port)
                if curr_node:
                    self.membership_list.update(packet.data)
                    self._notify_waiting(curr_node)

            elif packet.type == PacketType.PING:
                self.membership_list.update(packet.data)
                await self.io.send(host, port, Packet(PacketType.ACK, self.membership_list.get()).pack())

    async def _wait(self, node: Node, timeout: float) -> bool:
        event = Event()
        self._add_waiting(node, event)

        try:
            await asyncio.wait_for(event.wait(), timeout)
        except exceptions.TimeoutError:
            print(f'failed to recieve ACK from {node.host}:{node.port}')
        except Exception as e:
            print(f'Exception when waiting for ACK from {node.host}:{node.port}: {e}')
        finally:
            self.membership_list.update_node_status(node=node, status=0)
    
        return event.is_set()

    async def check(self, node: Node):
        # print(f'sending ping to {node.host}:{node.port}')
        self.membership_list.print()
        await self.io.send(node.host, node.port, Packet(PacketType.PING, self.membership_list.get()).pack())
        await self._wait(node, PING_TIMEOOUT)

    async def run_failure_detection(self) -> NoReturn:
        while True:
            for node in self.nodes:
                asyncio.create_task(self.check(node))

            # print(f'running failure detector: {datetime.now()}')
            await asyncio.sleep(PING_DURATION)

    @final
    async def run(self) -> NoReturn:
        await asyncio.gather(
            self._run_handler(),
            self.run_failure_detection())
        raise RuntimeError()
