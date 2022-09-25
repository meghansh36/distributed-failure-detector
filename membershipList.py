from datetime import datetime
from typing import List
from nodes import Node
import time
from nodes import Node

import logging

from config import CLEANUP_TIME, M, GLOBAL_RING_TOPOLOGY, Config, H7

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
                    logging.error(f'Failure Detected: {key}')
                    keys_for_cleanup.append(key)

        for key_for_cleanup in keys_for_cleanup:
            del self.memberShipListDict[key_for_cleanup]
            self._nodes_cleaned.add(key_for_cleanup)

        if len(self._nodes_cleaned) >= M:
            self.topology_change()
            self._nodes_cleaned.clear()

    
    def topology_change(self) -> bool:

        logging.warning(f'changing topology .........')

        online_nodes = [Config.get_node_from_unique_name(key) for key in self.memberShipListDict.keys()]
        # logging.debug(" ".join([node.unique_name for node in online_nodes]))

        if len(online_nodes) == 0:
            return False

        new_ping_nodes: List[Node] = []

        index = 0
        for current_pinging_node in self.current_pinging_nodes:
            # logging.debug(f'current ping node {current_pinging_node.unique_name}')
            if current_pinging_node in online_nodes:
                if current_pinging_node not in new_ping_nodes:
                    new_ping_nodes.append(current_pinging_node)
            else:
                curr_node = current_pinging_node
                replace_node = None
                found_replace_node = False
                while not found_replace_node:
                    curr_node_ping_list = GLOBAL_RING_TOPOLOGY[curr_node]
                    replace_node = curr_node_ping_list[index]
                    # logging.debug(f'checking nodes {curr_node.unique_name} {replace_node.unique_name}')
                    if (replace_node in online_nodes) and (replace_node not in new_ping_nodes) and (replace_node != self.itself):
                        found_replace_node = True
                        # logging.debug(f'found replacement node {replace_node.unique_name}')
                    elif replace_node is current_pinging_node:
                        break
                    else:
                        curr_node = replace_node
                
                if not found_replace_node:
                    break
            
                if current_pinging_node not in new_ping_nodes:
                    new_ping_nodes.append(replace_node)
                
            index += 1
        
        new_ping_nodes_str = ''
        for n in new_ping_nodes:
            new_ping_nodes_str += n.unique_name + ";"
        
        logging.info(f'new ping nodes: {new_ping_nodes_str}')

        self.current_pinging_nodes = new_ping_nodes
    
    def topology_change_for_new_node(self):

        logging.warning(f'new node detected. changing topology .........')

        online_nodes = [Config.get_node_from_unique_name(key) for key in self.memberShipListDict.keys()]

        actual_ping_nodes: List[Node] = GLOBAL_RING_TOPOLOGY[self.itself]
        
        new_ping_nodes: List[Node] = []

        index = 0
        for actual_ping_node in actual_ping_nodes:

            if (actual_ping_node in self.current_pinging_nodes) or (actual_ping_node in online_nodes):
                new_ping_nodes.append(actual_ping_node)
            else:
                new_ping_nodes.append(self.current_pinging_nodes[index])

            index += 1
        
        new_ping_nodes_str = ''
        for n in new_ping_nodes:
            new_ping_nodes_str += n.unique_name + ";"
        
        logging.info(f'new ping nodes: {new_ping_nodes_str}')
        
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
                    logging.debug(f'updating {key} to {new_time}, {new_status}')
                    self.memberShipListDict[key] = (new_time, new_status)
            else:
                logging.debug(f'new node added: {key}')
                self.memberShipListDict[key] = (new_time, new_status)

                if new_status:
                    isNewNodeAddedToList = True

        if isNewNodeAddedToList:
            self.topology_change()
            # self.topology_change_for_new_node()

    def update_node_status(self, node: Node, status: int):
        if node.unique_name in self.memberShipListDict:
            _, curr_node_status = self.memberShipListDict[node.unique_name]
            if curr_node_status:
                self.memberShipListDict[node.unique_name] = (time.time(), status)

    def print(self):
        items = list(self.memberShipListDict.items())
        items.sort()

        s = ""
        for key, value in items:
            s += f'{key} : {value}\n'
        logging.info(f"local membership list: \n{s}")

        s = ""
        for node in self.current_pinging_nodes:
            s += node.unique_name + ";"
        logging.info(f"current ping nodes: \n{s}")

