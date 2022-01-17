import abc
import collections
import logging
import time
import typing

import zmq

from zmq_broker.objects import Instruction

from home_library_common.logging.utility import setup_logger
from home_library_common.utility.exception import pdb_wrapped


class JobManagerInterface(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def add_worker(self, worker: typing.Optional[bytes]):
        ...

    @abc.abstractmethod
    def del_workers(self, workers: typing.Set[bytes]):
        ...

    @abc.abstractmethod
    def is_ready(self) -> bool:
        ...

    @abc.abstractmethod
    def get_status(self) -> typing.Dict:
        ...

    @abc.abstractmethod
    def get_worker(self, key: bytes) -> typing.Optional[bytes]:
        ...


class LRUJobManager(JobManagerInterface):
    def __init__(self):
        self._workers = set()
        self._unused_workers = set()
        self._inuse_workers = collections.OrderedDict()

    def add_worker(self, worker: typing.Optional[bytes]):
        if worker is None:
            return

        if worker in self._workers:
            return

        self._workers.add(worker)
        self._unused_workers.add(worker)

    def del_workers(self, workers: typing.Set[bytes]):
        if not workers:
            return

        self._workers = self._workers - workers
        self._unused_workers = self._unused_workers - workers
        dead_workers = {cache_key: worker for cache_key, worker in self._inuse_workers.items() if worker in workers}
        for cache_key, worker in dead_workers.items():
            self._inuse_workers.pop(cache_key)
            logging.info(f"delete worker {worker} for cache {cache_key}")

    def is_ready(self):
        return len(self._workers) > 0

    def get_status(self) -> typing.Dict:
        return {
            "workers": list(self._workers),
            "unused_workers": list(self._unused_workers),
            "inuse_workers": dict(self._inuse_workers),
        }

    def get_worker(self, key: bytes) -> typing.Optional[bytes]:
        if key in self._inuse_workers:
            worker = self._inuse_workers[key]
            self._inuse_workers.move_to_end(key, last=False)
        elif self._unused_workers:
            worker = self._unused_workers.pop()
        else:
            key_to_delete, worker = self._inuse_workers.popitem(last=True)
            logging.info(f"remove cache LRU cache key {key_to_delete}")

        self._inuse_workers[key] = worker
        return worker


class RoundRobinManager(JobManagerInterface):
    def __init__(self):
        self._workers = list()
        self._index = 0

    def add_worker(self, worker: typing.Optional[bytes]):
        if worker is None:
            return

        if worker in self._workers:
            return

        self._workers.append(worker)

    def del_workers(self, workers: typing.Set[bytes]):
        self._workers = list(set(self._workers) - workers)

    def is_ready(self) -> bool:
        return len(self._workers) > 0

    def get_status(self) -> typing.Dict:
        return {
            "workers": self._workers,
            "index": self._index % len(self._workers)
        }

    def get_worker(self, key: bytes) -> typing.Optional[bytes]:
        worker = self._workers[self._index % len(self._workers)]
        self._index += 1
        return worker


class Broker:
    def __init__(self, backend: str, frontend: str, ping_interval: int = 5):
        self._context = zmq.Context()
        self._backend = self._context.socket(zmq.ROUTER)
        self._backend.bind(backend)
        self._frontend = self._context.socket(zmq.ROUTER)
        self._frontend.bind(frontend)

        self._poller = zmq.Poller()
        self._poller.register(self._backend, zmq.POLLIN)
        self._poller.register(self._frontend, zmq.POLLIN)

        self._available_workers: typing.Dict[bytes, float] = dict()
        self._ping_interval: int = ping_interval

        self._response_record = collections.defaultdict(set)

        self.manager = LRUJobManager()

    def start(self):
        while True:
            [self._process_socket(socket) for socket, _ in self._poller.poll()]

    def _process_socket(self, socket: zmq.Socket):
        frames = socket.recv_multipart()
        self._update_workers_status(None)
        if socket == self._backend:
            if frames[1] == Instruction.HeartBeat.value:
                self._update_workers_status(frames[0])
                return
            self._process_backend(frames)
        elif socket == self._frontend:
            if frames[1] == Instruction.HeartBeat.value:
                return
            self._process_frontend(frames)

    def _update_workers_status(self, new_worker: typing.Optional[bytes]):
        now = time.monotonic()
        if new_worker is not None:
            self._available_workers[new_worker] = now

        cutoff = now - self._ping_interval
        dead_workers = {k for k, v in self._available_workers.items() if v < cutoff}
        self._available_workers = {k: v for k, v in self._available_workers.items() if k not in dead_workers}

        for dead_worker in dead_workers:
            if dead_worker not in self._response_record:
                continue

            for client in self._response_record.pop(dead_worker):
                self._frontend.send_multipart([client, dead_worker, Instruction.Dead.value, b""])

        self.manager.add_worker(new_worker)
        self.manager.del_workers(dead_workers)

    def _process_backend(self, frames: typing.List[bytes]):
        from_worker, to_client, instruction, function, result = frames
        logging.debug(f"route response to {to_client}, {function}, {result}")
        self._frontend.send_multipart([to_client, from_worker, Instruction.OK.value, result])

        self._response_record[from_worker].remove(to_client)
        if not self._response_record[from_worker]:
            self._response_record.pop(from_worker)

    def _process_frontend(self, frames: typing.List[bytes]):
        from_client, category_bytes, instruction, function, kwargs, key = frames
        if not self.manager.is_ready():
            self._frontend.send_multipart([from_client, self._frontend.identity, Instruction.NotReady.value, function])
            return

        to_worker = self._get_worker(from_client, category_bytes, function, key)
        logging.debug(f" route request to {to_worker}, {function}, {kwargs}")
        self._backend.send_multipart([to_worker, from_client, Instruction.Request.value, function, kwargs])
        self._response_record[to_worker].add(from_client)

    def _get_worker(self, from_client: bytes, category_bytes: bytes, function: bytes, key: bytes):
        logging.info(self.manager.get_status())
        return self.manager.get_worker(key)


@pdb_wrapped
def main():
    setup_logger()
    broker = Broker(backend="tcp://127.0.0.1:5000", frontend="tcp://127.0.0.1:5001")
    broker.start()
