from members import Member
import time

class MemberShipList:

    def __init__(self):
        self.memberShipList = {}

    def get_membership_list(self):
        return self.memberShipList

    def update_self_in_list(self, self_node: Member):

        node_data = (time.time(), "RUNNING")
        self.memberShipList[self_node] = node_data
    

