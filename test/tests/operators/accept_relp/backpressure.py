# runner: python
# timeout: 30

from __future__ import annotations

import os
import shlex
import socket
import struct
import subprocess
import threading
import time


def frame(transaction_id: int, command: str, payload: bytes = b"") -> bytes:
    header = f"{transaction_id} {command} {len(payload)}".encode()
    return header + (b" " + payload if payload else b"") + b"\n"


def recv_exact(sock: socket.socket, size: int) -> bytes:
    result = bytearray()
    while len(result) < size:
        chunk = sock.recv(size - len(result))
        if not chunk:
            raise EOFError
        result.extend(chunk)
    return bytes(result)


def recv_field(sock: socket.socket) -> tuple[bytes, bytes]:
    result = bytearray()
    while True:
        octet = recv_exact(sock, 1)
        if octet in {b" ", b"\n"}:
            return bytes(result), octet
        result.extend(octet)


def recv_response(sock: socket.socket) -> tuple[int, bytes]:
    transaction, delimiter = recv_field(sock)
    assert delimiter == b" "
    command, delimiter = recv_field(sock)
    assert command == b"rsp" and delimiter == b" "
    length, delimiter = recv_field(sock)
    size = int(length)
    assert delimiter == (b" " if size else b"\n")
    payload = recv_exact(sock, size) if size else b""
    if size:
        assert recv_exact(sock, 1) == b"\n"
    return int(transaction), payload


def main() -> None:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    pipeline = (
        f'accept_relp "127.0.0.1:{port}", max_message_size=64Mi '
        "| throttle rate=1, window=10s"
    )
    command = [
        *shlex.split(os.environ["TENZIR_BINARY"]),
        "--parallelism=1,fused=none",
        pipeline,
    ]
    process = subprocess.Popen(
        command,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        for _ in range(500):
            try:
                client = socket.create_connection(("127.0.0.1", port), timeout=0.1)
                break
            except OSError:
                if process.poll() is not None:
                    raise AssertionError(process.stderr.read())
                time.sleep(0.01)
        else:
            raise AssertionError("accept_relp did not start listening")
        time.sleep(0.05)
        client.setsockopt(
            socket.SOL_SOCKET,
            socket.SO_LINGER,
            struct.pack("ii", 1, 0),
        )
        client.sendall(frame(1, "open", b"relp_version=0\ncommands=syslog"))
        client.close()
        time.sleep(0.05)
        assert process.poll() is None, process.stderr.read()
        client = socket.create_connection(("127.0.0.1", port), timeout=2)
        with client:
            client.settimeout(2)
            client.sendall(frame(1, "open", b"relp_version=0\ncommands=syslog"))
            transaction, payload = recv_response(client)
            assert transaction == 1 and payload.startswith(b"200 ")
            client.sendall(frame(2, "syslog", b"x" * (40 * 1024 * 1024)))
            transaction, payload = recv_response(client)
            assert transaction == 2
            assert payload == b"200 OK"
            # Let the batch timeout push the large event into the blocked
            # downstream operator, then keep sending while responses are read.
            # Once every bounded handoff fills, the message queue must stop the
            # connection task before it acknowledges all transactions.
            time.sleep(0.1)
            num_messages = 20_000
            message = b"m" * (4 * 1024)
            sender_error: list[Exception] = []

            def send_messages() -> None:
                try:
                    for first in range(3, num_messages + 3, 1_000):
                        last = min(first + 1_000, num_messages + 3)
                        client.sendall(
                            b"".join(
                                frame(transaction, "syslog", message)
                                for transaction in range(first, last)
                            )
                        )
                except OSError as error:
                    sender_error.append(error)

            sender = threading.Thread(target=send_messages)
            sender.start()
            acknowledged = 0
            client.settimeout(0.2)
            while acknowledged < num_messages:
                try:
                    transaction, payload = recv_response(client)
                except socket.timeout:
                    break
                assert transaction == acknowledged + 3
                assert payload == b"200 OK"
                acknowledged += 1
            if acknowledged == num_messages:
                raise AssertionError(
                    "RELP acknowledged every transaction despite backpressure"
                )
            try:
                client.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            client.close()
            sender.join(timeout=2)
            assert not sender.is_alive()
            if sender_error:
                assert isinstance(sender_error[0], OSError)
        print("disconnect_contained: true")
        print("acknowledgement_stalled: true")
    finally:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)


if __name__ == "__main__":
    main()
