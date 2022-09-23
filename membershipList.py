from nodes import Node
import time
from nodes import Node

from config import CLEANUP_TIME

class MemberShipList:

    def __init__(self, node: Node):
        self.memberShipListDict = {}
        self.self_node: Node = node
    
    def _cleanup(self):

        for key in self.memberShipListDict.keys():
            node_curr_time, node_curr_status = self.memberShipListDict[key]

            if not node_curr_status and (time.time() - node_curr_time >= CLEANUP_TIME):
                print(f'failure detected: {key.name}')
                del self.memberShipListDict[key]

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
                    self.memberShipListDict[key] = (new_time, new_status)
            else:
                self.memberShipListDict[key] = (new_time, new_status)

                if new_status:
                    isNewNodeAddedToList = True

    def update_node_status(self, node: Node, status: int):
        if node.unique_name in self.memberShipListDict:
            self.memberShipListDict[node.unique_name][1] = status
    
    def print(self):
        print(f'current local membership list')
        for key, value in self.get().items():
            print(f'{key} : {value}')

