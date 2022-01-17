import logging
import multiprocessing
import os
import pickle
import socket
import threading
import time

import zmq

from zmq_broker.objects import Instruction, Category
from zmq_broker.utility import start_ping

from home_library_common.logging.utility import setup_logger


def get_data(run_id: int):
    time.sleep(run_id)
    return run_id


class Worker:
    def __init__(self, broker: str, category: Category, ping_interval: int = 1):
        self._category = category

        self._broker = broker
        self._context = zmq.Context()
        self._socket = self._context.socket(zmq.DEALER)
        self._socket.setsockopt(zmq.IDENTITY, f"Worker|{socket.gethostname()}|{os.getpid()}".encode())
        self._socket.connect(self._broker)

        self._stop_event = threading.Event()
        self._ping_thread = start_ping(self._socket, ping_interval, stop_event=self._stop_event)

    def __del__(self):
        self._stop_event.set()
        self._ping_thread.join()

    def start(self):
        while True:
            self._process_one_request()

    def _register_category(self):
        pass

    def _process_one_request(self):
        frames = self._socket.recv_multipart()
        client, instruction, function_bytes, kwargs_bytes = frames

        kwargs = pickle.loads(kwargs_bytes)
        logging.info(f"sleep for {kwargs['delay']} seconds")
        time.sleep(kwargs["delay"])

        self._socket.send_multipart([client, Instruction.OK.value, function_bytes, kwargs_bytes])


def start_worker():
    setup_logger()
    worker = Worker("tcp://127.0.0.1:5000", Category.QueryData)
    worker.start()


def main():
    with multiprocessing.Pool(1) as pool:
        pool.starmap(start_worker, [tuple(), tuple()])
