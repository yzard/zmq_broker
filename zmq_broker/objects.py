import enum


class Instruction(enum.Enum):
    HeartBeat = b"HB"
    RegisterCategory = b"RC"
    NotSupportedCategory = b"NS"
    NoWorkers = b"NR"
    Dead = b"DD"
    Request = b"RQ"
    OK = b"OK"


class Category(enum.Enum):
    QueryData = b"QD"
