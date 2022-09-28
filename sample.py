from multiprocessing import Process, Queue
from queue import Empty
from time import sleep

from edelweiss_multiprocess.main import (
    connect_socket,
    get_instrument_token,
    make_params,
    stream,
)


def print_ltp(ltp: Queue):
    try:
        print(ltp.get(block=False))
    except Empty:
        return


if __name__ == "__main__":
    tokens = get_instrument_token("instruments.csv", 1000)
    socket = connect_socket()
    params = make_params(tokens)
    queue = Queue()
    p = Process(target=stream, args=(socket, params, queue))
    p.start()
    while True:
        p1 = Process(target=print_ltp, args=(queue,))
        p1.start()
        sleep(1)
