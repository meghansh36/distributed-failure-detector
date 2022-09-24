from enum import Enum
import pickle
import struct
import json

class PacketType(str, Enum):

    PING = "01"
    ACK = "10"
    INTRODUCE = "00"

class Packet:
    def __init__(self, sender: str, packetType: PacketType, data: dict):
        self.data = data
        self.type = packetType
        self.sender = sender

    def pack(self):
        jsondata = json.dumps(self.data)
        return struct.pack(f"i{255}s{2}si{2048}s", len(self.sender), self.sender.encode('utf-8'), self.type.encode('utf-8'), len(jsondata), jsondata.encode())

        # pickled = pickle.dumps(self, pickle.HIGHEST_PROTOCOL)
        # return pickled

    @staticmethod
    def unpack(recvPacket: bytes):
        try:
            unpacked_tuple: tuple[bytearray] = struct.unpack(f"i{255}s{2}si{2048}s", recvPacket)
            sender = unpacked_tuple[1][:unpacked_tuple[0]].decode('utf-8')
            packetType = unpacked_tuple[2].decode('utf-8')
            data = unpacked_tuple[4][:unpacked_tuple[3]].decode('utf-8')

            # print(sender, packetType, data)
            return Packet(sender, PacketType(packetType), json.loads(data))
        except Exception as e:
            print("unknown bytes.", e)
            return None
