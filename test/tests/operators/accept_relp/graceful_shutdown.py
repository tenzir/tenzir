# runner: python
# timeout: 30

from __future__ import annotations

import json
import os
import shlex
import socket
import subprocess
import tempfile
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
        f'accept_relp "127.0.0.1:{port}" '
        "| schema = @name | select data, relp, schema | write_ndjson"
    )
    command = [
        *shlex.split(os.environ["TENZIR_BINARY"]),
        "--nova=true",
        "--parallelism=1,fused=none",
        pipeline,
    ]
    output = tempfile.TemporaryFile(mode="w+")
    process = subprocess.Popen(
        command,
        stdout=output,
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
        with client:
            client.settimeout(2)
            client.sendall(frame(1, "open", b"relp_version=0\ncommands=syslog"))
            transaction, payload = recv_response(client)
            assert transaction == 1 and payload.startswith(b"200 ")
            # Cross the 1,024-event batch boundary twice, then stop immediately
            # after the final acknowledgement to exercise draining the tail.
            count = 2_050
            client.sendall(
                b"".join(
                    frame(index + 2, "syslog", f"message-{index}".encode())
                    for index in range(count)
                )
            )
            for index in range(count):
                transaction, payload = recv_response(client)
                assert transaction == index + 2 and payload == b"200 OK"
            process.terminate()
            _, stderr = process.communicate(timeout=5)
            assert process.returncode == 0, stderr
            output.seek(0)
            events = [json.loads(line) for line in output]
            assert events == [
                {
                    "data": f"message-{index}",
                    "relp": {"transaction_id": index + 2},
                    "schema": "tenzir.accept_relp",
                }
                for index in range(count)
            ]
        print("accepted_messages_drained: true")
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)
        output.close()


if __name__ == "__main__":
    main()
