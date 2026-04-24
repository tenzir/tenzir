from __future__ import annotations

import argparse
import signal
import time
from pathlib import Path

import zmq

_STARTUP_SETTLE_SECONDS = 0.25
_RECV_POLL_TIMEOUT_MS = 100
_running = True


def _handle_stop(_: int, __: object) -> None:
    global _running
    _running = False


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--role", choices=("publisher", "subscriber"), required=True)
    parser.add_argument("--mode", choices=("bind", "connect"), required=True)
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--ready-file", required=True)
    parser.add_argument("--capture-file", required=True)
    parser.add_argument("--payload", default="")
    parser.add_argument("--subscribe-prefix", default="")
    parser.add_argument("--send-interval-ms", type=int, default=50)
    args = parser.parse_args()

    signal.signal(signal.SIGTERM, _handle_stop)
    signal.signal(signal.SIGINT, _handle_stop)

    context = zmq.Context()
    socket_type = zmq.PUB if args.role == "publisher" else zmq.SUB
    socket = context.socket(socket_type)
    socket.setsockopt(zmq.LINGER, 0)
    if args.role == "subscriber":
        socket.setsockopt_string(zmq.SUBSCRIBE, args.subscribe_prefix)
    if args.mode == "bind":
        socket.bind(args.endpoint)
    else:
        socket.connect(args.endpoint)
    Path(args.ready_file).touch()
    time.sleep(_STARTUP_SETTLE_SECONDS)
    try:
        if args.role == "publisher":
            payload = args.payload.encode()
            interval = args.send_interval_ms / 1000.0
            while _running:
                socket.send(payload)
                time.sleep(interval)
        else:
            poller = zmq.Poller()
            poller.register(socket, zmq.POLLIN)
            capture_path = Path(args.capture_file)
            while _running:
                if socket not in dict(poller.poll(_RECV_POLL_TIMEOUT_MS)):
                    continue
                payload = socket.recv()
                with capture_path.open("ab") as capture:
                    capture.write(payload)
                    capture.write(b"\n")
    finally:
        socket.close()
        context.term()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
