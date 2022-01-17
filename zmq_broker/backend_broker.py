import abc
import collections
import logging
import time
import typing

import zmq

from zmq_broker.objects import Instruction, Category

from home_library_common.logging.utility import setup_logger
from home_library_common.utility.exception import pdb_wrapped


class JobManager(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def add_worker(self, worker: typing.Optional[bytes]):
        ...

    @abc.abstractmethod
    def del_workers(self, workers: typing.Set[bytes]):
        ...

    @abc.abstractmethod
    def get_registered_workers(self) -> typing.Set[bytes]:
        ...

    @abc.abstractmethod
    def get_status(self) -> typing.Dict:
        ...

    @abc.abstractmethod
    def get_worker(self, key: bytes) -> typing.Optional[bytes]:
        ...


class LRUJobManager(JobManager):
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

    def get_registered_workers(self) -> typing.Set[bytes]:
        return self._workers

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


class RoundRobinManager(JobManager):
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

    def get_registered_workers(self) -> typing.Set[bytes]:
        return set(self._workers)

    def get_status(self) -> typing.Dict:
        return {"workers": self._workers, "index": self._index % len(self._workers)}

    def get_worker(self, key: bytes) -> typing.Optional[bytes]:
        worker = self._workers[self._index % len(self._workers)]
        self._index += 1
        return worker


class Broker:
    def __init__(
        self, backend: str, frontend: str, category_managers: typing.Dict[bytes, JobManager], ping_interval: int = 5,
    ):
        self._context = zmq.Context()
        self._backend: zmq.Socket = self._context.socket(zmq.ROUTER)
        self._backend.bind(backend)
        self._frontend: zmq.Socket = self._context.socket(zmq.ROUTER)
        self._frontend.bind(frontend)

        self._poller = zmq.Poller()
        self._poller.register(self._backend, zmq.POLLIN)
        self._poller.register(self._frontend, zmq.POLLIN)

        self._response_record = collections.defaultdict(set)

        self._heartbeat_check_interval: int = ping_interval
        self._workers_last_heartbeat: typing.Dict[bytes, float] = dict()
        self._category_managers = category_managers
        self._worker_to_manager: typing.Dict[bytes, JobManager] = dict()

    def start(self):
        while True:
            [self._process_socket(socket) for socket, _ in self._poller.poll()]

    def _process_socket(self, socket: zmq.Socket):
        frames = socket.recv_multipart()
        instruction_bytes = frames[1]
        self._update_workers_status()
        if instruction_bytes == Instruction.HeartBeat.value:
            self._fresh_worker_heartbeat(frames[0])
            return

        if socket == self._backend:
            if instruction_bytes == Instruction.RegisterCategory.value:
                self._register_new_worker(frames[0], frames[2])
                return

            self._process_backend(frames)
        elif socket == self._frontend:
            self._process_frontend(frames)

    def _update_workers_status(self):
        now = time.monotonic()
        cutoff = now - self._heartbeat_check_interval
        dead_workers = {k for k, v in self._workers_last_heartbeat.items() if v < cutoff}

        for dead_worker in dead_workers:
            if dead_worker not in self._response_record:
                continue

            for client in self._response_record.pop(dead_worker):
                self._send_to_frontend(client, Instruction.Dead, b"")

        self._workers_last_heartbeat = {k: v for k, v in self._workers_last_heartbeat.items() if k not in dead_workers}

        manager_to_dead_workers = collections.defaultdict(set)
        for dead_worker in dead_workers:
            manager_to_dead_workers[self._worker_to_manager.pop(dead_worker)].add(dead_worker)

        for manager, to_be_removed_workers in manager_to_dead_workers.items():
            logging.info(f"removed {len(to_be_removed_workers)} workers from {manager}")
            manager.del_workers(to_be_removed_workers)

    def _fresh_worker_heartbeat(self, worker):
        if worker not in self._workers_last_heartbeat:
            return

        self._workers_last_heartbeat[worker] = time.monotonic()

    def _register_new_worker(self, new_worker: bytes, category_bytes: bytes):
        manager = self._category_managers.get(category_bytes)
        if manager is None:
            self._send_to_backend(new_worker, self._backend.identity, Instruction.NotSupportedCategory, b"", b"")
            return

        self._workers_last_heartbeat[new_worker] = time.monotonic()
        self._worker_to_manager[new_worker] = manager
        manager.add_worker(new_worker)
        logging.info(
            f"registered worker {new_worker} to {Category(category_bytes)}: {len(manager.get_registered_workers())} "
            f"workers"
        )

    def _process_backend(self, frames: typing.List[bytes]):
        from_worker, to_client, instruction, function, result = frames
        logging.debug(f"route response to {to_client}, {function}, {result}")
        self._send_to_frontend(to_client, Instruction.OK, result)

        self._response_record[from_worker].remove(to_client)
        if not self._response_record[from_worker]:
            self._response_record.pop(from_worker)

    def _process_frontend(self, frames: typing.List[bytes]):
        from_client, _, instruction, category_bytes, function, kwargs, key = frames
        manager = self._category_managers.get(category_bytes)
        if manager is None:
            self._send_to_frontend(from_client, Instruction.NotSupportedCategory, category_bytes)
            return

        if not manager.get_registered_workers():
            self._send_to_frontend(from_client, Instruction.NoWorkers, category_bytes)
            return

        to_worker = self._get_worker(category_bytes, key)
        logging.debug(f"route request to {to_worker}, {function}, {kwargs}")
        self._send_to_backend(to_worker, from_client, Instruction.Request, function, kwargs)
        self._response_record[to_worker].add(from_client)

    def _get_worker(self, category_bytes: bytes, key: bytes):
        manager = self._category_managers[category_bytes]
        logging.info(manager.get_status())
        return manager.get_worker(key)

    def _send_to_backend(self, worker: bytes, client: bytes, instruction: Instruction, function: bytes, kwargs: bytes):
        self._backend.send_multipart([worker, client, instruction.value, function, kwargs])

    def _send_to_frontend(self, client: bytes, instruction: Instruction, data: bytes):
        self._frontend.send_multipart([client, self._frontend.identity, instruction.value, data])


@pdb_wrapped
def main():
    setup_logger()
    broker = Broker(
        backend="tcp://127.0.0.1:5000",
        frontend="tcp://127.0.0.1:5001",
        category_managers={Category.QueryData.value: LRUJobManager()},
    )
    broker.start()
