from datetime import datetime
import logging
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

    def __init__(self, io: AwesomeProtocol) -> None:
        self.io: Final = io
        self._waiting: WeakKeyDictionary[Node, WeakSet[Event]] = WeakKeyDictionary()
        self.config: Config = None
        self.membership_list: Optional[MemberShipList] = None
    
    def initialize(self, config: Config):
        self.config = config
        self.waiting_for_introduction = False if self.config.introducerFlag else True
        self.membership_list = MemberShipList(self.config.node, self.config.ping_nodes)
    

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
            
            logging.debug(f'got data: {packet.data} from {host}:{port}')

            if packet.type == PacketType.ACK:
                curr_node = Config.get_node_from_unique_name(packet.sender)
                logging.debug(f'got ack from {curr_node}')
                if curr_node:
                    self.membership_list.update(packet.data)
                    self._notify_waiting(curr_node)

            elif packet.type == PacketType.PING or packet.type == PacketType.INTRODUCE:
                # print(f'{datetime.now()}: received ping from {host}:{port}')
                self.membership_list.update(packet.data)
                await self.io.send(host, port, Packet(self.config.node.unique_name, PacketType.ACK, self.membership_list.get()).pack())

    async def _wait(self, node: Node, timeout: float) -> bool:
        event = Event()
        self._add_waiting(node, event)

        try:
            await asyncio.wait_for(event.wait(), timeout)
        except exceptions.TimeoutError:
            # print(f'{datetime.now()}: failed to recieve ACK from {node.unique_name}')
            logging.error(f'failed to recieve ACK from {node.unique_name}')
            if not self.waiting_for_introduction:
                self.membership_list.update_node_status(node=node, status=0)
            else:
                logging.error("Introducer Timed Out")
        except Exception as e:
            # print(f'{datetime.now()}: Exception when waiting for ACK from {node.unique_name}: {e}')
            logging.error(f'Exception when waiting for ACK from {node.unique_name}: {e}')
            if not self.waiting_for_introduction:
                self.membership_list.update_node_status(node=node, status=0)
            else:
                logging.error(f'{datetime.now()}: Exception when waiting for ACK from introducer: {e}')
    
        return event.is_set()

    async def introduce(self):
        logging.debug(f'sending pings to introducer: {self.config.introducerNode.unique_name}')
        await self.io.send(self.config.introducerNode.host, self.config.introducerNode.port, Packet(self.config.node.unique_name, PacketType.INTRODUCE, self.membership_list.get()).pack())
        await self._wait(self.config.introducerNode, PING_TIMEOOUT)

    async def check(self, node: Node):
        logging.debug(f'pinging: {node.unique_name}')
        await self.io.send(node.host, node.port, Packet(self.config.node.unique_name, PacketType.PING, self.membership_list.get()).pack())
        await self._wait(node, PING_TIMEOOUT)

    async def run_failure_detection(self) -> NoReturn:
        while True:
            if not self.waiting_for_introduction:
                for node in self.membership_list.current_pinging_nodes:
                    asyncio.create_task(self.check(node))
            else:
                asyncio.create_task(self.introduce())

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

    @final
    async def run(self) -> NoReturn:
        await asyncio.gather(
            self._run_handler(),
            self.run_failure_detection(),
            self.check_user_input())
        raise RuntimeError()
