from nodes import Node
import time
from nodes import Node

class MemberShipList:

    def __init__(self, node: Node):
        self.memberShipList = {}
        self.self_node: Node = node

    def get(self):
        node_data = (time.time(), 1)
        # DO A CLEANUP
        self.memberShipList[self.self_node] = node_data
        return self.memberShipList

    def update(self, new_membership_list: dict):
        pass

    

