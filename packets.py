from enum import Enum
import pickle

class PacketType(Enum):

    PING = 1
    ACK = 2
    INTRODUCE = 3

class Packet:
    def __init__(self, packetType: PacketType, data: dict):
        self.data = data
        self.type = packetType

    def pack(self):
        pickled = pickle.dumps(self, pickle.HIGHEST_PROTOCOL)
        return pickled

    @staticmethod
    def unpack(recvPickle: bytes):
        try:
            packet = pickle.loads(recvPickle)
            return packet
        except:
            print("unknown bytes.")
            return None
