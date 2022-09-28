import csv
import json
import logging
import socket
from queue import Empty, Queue
from typing import Any, Dict, List

from edelweiss_multiprocess.constants import APPID, HOST, PORT

logging.basicConfig(level=logging.DEBUG)


def connect_socket(host: str = HOST, port: int = PORT) -> socket.SocketType:
    sock = socket.create_connection((host, port))
    logging.debug("socket connected")
    return sock


def stream(sock: socket.SocketType, params: Dict[str, Any], queue: Queue):
    sock.sendall(bytes(json.dumps(params) + "\n", "UTF-8"))

    while True:
        data = sock.recv(1024)
        if data:
            try:
                data_dict = json.loads(data.decode("UTF-8"))
            except json.decoder.JSONDecodeError:
                logging.error("JSONDecodeError")
                continue
            ltp = {
                data_dict["response"]["data"]["z3"]: data_dict["response"]["data"]["a9"]
            }
            try:
                temp = queue.get(block=False)
            except Empty:
                temp = []
            temp.append(ltp)
            queue.put(temp, block=False)


def make_params(symbols: List[dict]) -> Dict[str, Any]:
    return {
        "request": {
            "streaming_type": "quote3",
            "data": {
                "accType": "EQ",
                "symbols": symbols,
            },
            "formFactor": "M",
            "appID": APPID,
            "response_format": "json",
            "request_type": "subscribe",
        },
        "echo": {},
    }


def get_instrument_token(file: str, size: int):
    count = 0
    tokens = []
    with open(file, "r") as f:
        instruments = csv.DictReader(f)
        for row in instruments:
            if row["exchangetoken"]:
                count = count + 1
                tokens.append(row["exchangetoken"])
                if count >= size:
                    break
    return [{"symbol": str(token)} for token in tokens]
