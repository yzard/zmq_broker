import threading
import time

import zmq

from zmq_broker.objects import Instruction


def start_ping(socket: zmq.Socket, interval: int, stop_event: threading.Event) -> threading.Thread:
    def ping_thread(event: threading.Event):
        while not event.is_set():
            time.sleep(interval)
            socket.send(Instruction.HeartBeat.value)

    thread = threading.Thread(target=ping_thread, args=(stop_event,))
    thread.start()
    return thread
