from datetime import datetime
from nodes import Node
import time
from nodes import Node

from config import CLEANUP_TIME

class MemberShipList:

    def __init__(self, node: Node):
        self.memberShipListDict = {}
        self.self_node: Node = node
    
    def _cleanup(self):

        keys_for_cleanup = []
        for key in self.memberShipListDict.keys():
            node_curr_time, node_curr_status = self.memberShipListDict[key]

            if not node_curr_status:
                if (time.time() - node_curr_time) >= CLEANUP_TIME:
                    print(f'failure detected: {key}')
                    keys_for_cleanup.append(key)
        
        for key_for_cleanup in keys_for_cleanup:
            del self.memberShipListDict[key_for_cleanup]

    def get(self):
        self._cleanup()
        self.memberShipListDict[self.self_node.unique_name] = (time.time(), 1)
        return self.memberShipListDict

    def update(self, new_membership_list: dict) -> None:

        isNewNodeAddedToList = False
        for key in new_membership_list.keys():
            new_time, new_status = new_membership_list[key]
            if key in self.memberShipListDict:
                curr_time, curr_status = self.memberShipListDict[key]
                if curr_time < new_time:
                    print(f'{datetime.now()}: updating {key} to {new_time}, {new_status}')
                    self.memberShipListDict[key] = (new_time, new_status)
                    self.print()
            else:
                print(f'{datetime.now()}: freshly adding {key}')
                self.memberShipListDict[key] = (new_time, new_status)
                self.print()

                if new_status:
                    isNewNodeAddedToList = True

    def update_node_status(self, node: Node, status: int):
        if node.unique_name in self.memberShipListDict:
            _, curr_node_status = self.memberShipListDict[node.unique_name]
            if curr_node_status:
                self.memberShipListDict[node.unique_name] = (time.time(), status)
    
    def print(self):
        print(f'current local membership list')
        for key, value in self.memberShipListDict.items():
            print(f'{key} : {value}')

