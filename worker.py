from datetime import datetime
import sys
import asyncio
from asyncio import Event, exceptions
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
                curr_node = Config.get_node_from_unique_name(packet.sender)
                print(f'{datetime.now()}: got ack from {curr_node}')
                if curr_node:
                    self.membership_list.update(packet.data)
                    self._notify_waiting(curr_node)

            elif packet.type == PacketType.PING:
                print(f'{datetime.now()}: received ping from {host}:{port}')
                self.membership_list.update(packet.data)
                await self.io.send(host, port, Packet(self.config.node.unique_name, PacketType.ACK, self.membership_list.get()).pack())

    async def _wait(self, node: Node, timeout: float) -> bool:
        event = Event()
        self._add_waiting(node, event)

        try:
            await asyncio.wait_for(event.wait(), timeout)
        except exceptions.TimeoutError:
            print(f'{datetime.now()}: failed to recieve ACK from {node.unique_name}')
            self.membership_list.update_node_status(node=node, status=0)
        except Exception as e:
            print(f'Exception when waiting for ACK from {node.unique_name}: {e}')
            self.membership_list.update_node_status(node=node, status=0)
    
        return event.is_set()

    async def check(self, node: Node):
        # print(f'sending ping to {node.host}:{node.port}')
        print(f'{datetime.now()}: pingning {node.unique_name}')
        await self.io.send(node.host, node.port, Packet(self.config.node.unique_name, PacketType.PING, self.membership_list.get()).pack())
        await self._wait(node, PING_TIMEOOUT)

    async def run_failure_detection(self) -> NoReturn:
        while True:
            print('-------------------------------')
            for node in self.nodes:
                asyncio.create_task(self.check(node))

            # print(f'running failure detector: {datetime.now()}')
            await asyncio.sleep(PING_DURATION)

    async def check_user_input(self):
        loop = asyncio.get_event_loop()
        queue = asyncio.Queue()

        def response():
            loop.create_task(queue.put(sys.stdin.readline()))

        loop.add_reader(sys.stdin.fileno(), response)

        while True:
            data = await queue.get()
            self.membership_list.print()
            # print("got: " + data.strip())

    @final
    async def run(self) -> NoReturn:
        await asyncio.gather(
            self._run_handler(),
            self.run_failure_detection(),
            self.check_user_input())
        raise RuntimeError()
