from datetime import datetime
from typing import List
from nodes import Node
import time
from nodes import Node

from config import CLEANUP_TIME, M, GLOBAL_RING_TOPOLOGY, Config

class MemberShipList:

    def __init__(self, node: Node, ping_nodes: List[Node]):
        self.memberShipListDict = {}
        self.itself: Node = node
        self.current_pinging_nodes: List[Node] = ping_nodes
        self._nodes_cleaned = set()
    
    def _cleanup(self):

        keys_for_cleanup = []
        for key in self.memberShipListDict.keys():
            node_curr_time, node_curr_status = self.memberShipListDict[key]

            if not node_curr_status:
                if (time.time() - node_curr_time) >= CLEANUP_TIME:
                    print(f'{datetime.now()}: Failure Detected: {key}')
                    keys_for_cleanup.append(key)

        for key_for_cleanup in keys_for_cleanup:
            del self.memberShipListDict[key_for_cleanup]
            self._nodes_cleaned.add(key_for_cleanup)

        if len(self._nodes_cleaned) >= M:
            self.topology_change()
            self._nodes_cleaned.clear()

    
    def topology_change(self) -> bool:

        print('changing topology .........')

        online_nodes = [Config.get_node_from_unique_name(key) for key in self.memberShipListDict.keys()]
        if len(online_nodes) == 0:
            return False

        new_ping_nodes: List[Node] = []

        index = 0
        for current_pinging_node in self.current_pinging_nodes:
            
            if current_pinging_node in online_nodes:
                new_ping_nodes.append(current_pinging_node)
            else:
                curr_node = current_pinging_node
                replace_node = None
                found_replace_node = False
                while not found_replace_node:
                    curr_node_ping_list = GLOBAL_RING_TOPOLOGY[curr_node]
                    replace_node = curr_node_ping_list[index]
                    if (replace_node in online_nodes) and (replace_node not in new_ping_nodes) and (replace_node != self.itself):
                        found_replace_node = True
                    else:
                        curr_node = replace_node
                
                if not found_replace_node:
                    break
            
                new_ping_nodes.append(replace_node)

            index += 1
        
        print('new nodes:')
        for n in new_ping_nodes:
            print(f'{n.unique_name}')

        self.current_pinging_nodes = new_ping_nodes
            

    def get(self):
        self._cleanup()
        self.memberShipListDict[self.itself.unique_name] = (time.time(), 1)
        return self.memberShipListDict

    def update(self, new_membership_list: dict) -> None:

        isNewNodeAddedToList = False
        for key in new_membership_list.keys():
            new_time, new_status = new_membership_list[key]
            if key in self.memberShipListDict:
                curr_time, curr_status = self.memberShipListDict[key]
                if curr_time < new_time:
                    # print(f'{datetime.now()}: updating {key} to {new_time}, {new_status}')
                    self.memberShipListDict[key] = (new_time, new_status)
                    # self.print()
            else:
                # print(f'{datetime.now()}: freshly adding {key}')
                self.memberShipListDict[key] = (new_time, new_status)
                # self.print()

                if new_status:
                    isNewNodeAddedToList = True

        # if isNewNodeAddedToList:
        #     self.topology_change()

    def update_node_status(self, node: Node, status: int):
        if node.unique_name in self.memberShipListDict:
            _, curr_node_status = self.memberShipListDict[node.unique_name]
            if curr_node_status:
                self.memberShipListDict[node.unique_name] = (time.time(), status)

    def print(self):
        items = list(self.memberShipListDict.items())
        items.sort()
        print(f'local membership list: {len(items)}')
        for key, value in items:
            print(f'{key} : {value}')

