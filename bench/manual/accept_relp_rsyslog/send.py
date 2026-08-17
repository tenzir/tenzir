#!/usr/bin/env python3
import argparse
import json
import socket
import time


def make_message() -> bytes:
    return (
        b"<86>1 2026-08-16T12:00:00.123456Z bastion-01 sshd 4242 auth - "
        b"Failed password for invalid user admin from 198.51.100.42 port 49152 ssh2\n"
    )


def connect(host: str, port: int) -> socket.socket:
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        try:
            return socket.create_connection((host, port), timeout=5)
        except OSError:
            time.sleep(0.05)
    raise TimeoutError(f"could not connect to {host}:{port}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("count", type=int)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=1514)
    args = parser.parse_args()
    message = make_message()
    batch_size = min(1000, args.count)
    batch = message * batch_size
    full_batches, remainder = divmod(args.count, batch_size)
    started = time.monotonic()
    with connect(args.host, args.port) as sock:
        for _ in range(full_batches):
            sock.sendall(batch)
        if remainder:
            sock.sendall(message * remainder)
        sock.shutdown(socket.SHUT_WR)
    elapsed = time.monotonic() - started
    print(
        json.dumps(
            {
                "events": args.count,
                "syslog_message_bytes": len(message) - 1,
                "input_bytes": len(message) * args.count,
                "send_seconds": elapsed,
            }
        )
    )


if __name__ == "__main__":
    main()
