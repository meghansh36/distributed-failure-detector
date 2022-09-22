import pickle
class Packet:
    def __init__(self, packetType: str, data: str):
        self.data = data
        self.packetType = packetType

    def packet_pack(self):
        pickled = pickle.dumps(self, pickle.HIGHEST_PROTOCOL)
        return pickled

    @staticmethod
    def packet_unpack(recvPickle):
        packet = pickle.loads(recvPickle)
        return packet
