import enum


class Instruction(enum.Enum):
    HeartBeat = b"H"
    Register = b"T"
    NotReady = b"N"
    Dead = b"D"
    Request = b"R"
    OK = b"P"


class Category(enum.Enum):
    QueryData = b"Q"
