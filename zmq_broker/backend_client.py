import logging
import multiprocessing
import os
import pickle
import random
import socket
import sys
import threading
import typing

import zmq

from zmq_broker.utility import start_ping
from zmq_broker.objects import Instruction, Category

from home_library_common.logging.utility import setup_logger


class Client:
    def __init__(self, broker: str, ping_interval: int = 1):
        self._broker = broker
        self._context = zmq.Context()
        self._socket = self._context.socket(zmq.DEALER)
        self._socket.setsockopt(zmq.IDENTITY, f"Client|{socket.gethostname()}|{os.getpid()}".encode())
        self._socket.connect(self._broker)

        self._stop_event = threading.Event()
        self._ping_thread = start_ping(self._socket, ping_interval, self._stop_event)

    def close(self):
        self._stop_event.set()
        self._ping_thread.join()

    def request(self, category: Category, function: str, route: bytes = bytes(), **kwargs):
        self._socket.send_multipart(
            [Instruction.Request.value, category.value, function.encode(), pickle.dumps(kwargs), route]
        )
        frames = self._socket.recv_multipart()
        client, instruction_bytes, result_bytes = frames
        instruction = Instruction(instruction_bytes)
        if instruction != Instruction.OK:
            raise IOError(f"request error: status={instruction.name} {function=}, {route=}, {kwargs=}")

        result = pickle.loads(result_bytes)
        if kwargs != result:
            logging.info(f"{self._socket.identity}: request and results are not the same")

        logging.info(f"{self._socket.identity}: get {result=}")


def start_client():
    setup_logger()
    client = Client("tcp://127.0.0.1:5001")

    random.seed()

    key = sys.argv[1]
    while True:
        client.request(Category.QueryData, "get_data", key.encode(), delay=random.randint(0, 4))


def main():
    start_client()
