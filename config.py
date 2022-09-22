from typing import final, List
from members import Member

M : final = 4

PING_TIMEOOUT: final = 0.5

PING_DURATION: final = 1.5

H1: final = Member('127.0.0.1', 8001)
H2: final = Member('127.0.0.1', 8002)
H3: final = Member('127.0.0.1', 8003)
H4: final = Member('127.0.0.1', 8004)
H5: final = Member('127.0.0.1', 8005)
H6: final = Member('127.0.0.1', 8006)
H7: final = Member('127.0.0.1', 8007)
H8: final = Member('127.0.0.1', 8008)
H9: final = Member('127.0.0.1', 8009)
H10: final = Member('127.0.0.1', 8010)

GLOBAL_RING_TOPOLOGY: dict = {

    H1: [H2, H10, H8, H6],

    H2: [H3, H1, H9, H7],

    H3: [H4, H2, H9, H8],

    H4: [H5, H3, H8, H10],

    H5: [H6, H4, H9, H1],

    H6: [H7, H5, H1, H3],

    H7: [H8, H6, H2, H4],

    H8: [H9, H7, H3, H4],

    H9: [H10, H8, H3, H5],

    H10: [H1, H9, H4, H6]

}


class Config:

    def __init__(self, hostname, port) -> None:
        
        self.member: Member = Config.get_member(hostname, port)
        self.ping_members: List[Member] = GLOBAL_RING_TOPOLOGY[self.member]
        
    @staticmethod
    def get_member(hostname, port) -> Member:

        member = None
        for node in GLOBAL_RING_TOPOLOGY.keys():
            if node.host == hostname and node.port == port:
                member = node
        
        return member
    
