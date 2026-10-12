# runner: python
# timeout: 180

from __future__ import annotations

import http.client
import json
import os
import re
import shlex
import subprocess
import threading
import time


def _bound_port(
    process: subprocess.Popen[str], listening: threading.Event, ports: list[int]
) -> int:
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        if listening.wait(0.1):
            return ports[0]
        if process.poll() is not None:
            raise AssertionError("serve_http exited before reporting its port")
    raise AssertionError("serve_http did not report its port within 30 seconds")


def _terminate(process: subprocess.Popen[str]) -> None:
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()


def _write(process: subprocess.Popen[str], data: str) -> None:
    assert process.stdin is not None
    process.stdin.write(json.dumps({"data": data}) + "\n")
    process.stdin.flush()


def _run_case(lines: list[str]) -> None:
    prefixes = [
        "".join(f"{line}\n" for line in lines[:i]).encode()
        for i in range(1, len(lines) + 1)
    ]
    pipeline = """
from_stdin { read_ndjson }
batch 1
serve_http "127.0.0.1:0" { write_lines }
""".strip()
    process = subprocess.Popen(
        [
            *shlex.split(os.environ["TENZIR_BINARY"]),
            "--bare-mode",
            "--console-verbosity=debug",
            "--nova=true",
            "--multi",
            pipeline,
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    assert process.stderr is not None
    stderr_pipe = process.stderr
    process.stderr = None  # The log reader owns the pipe, not communicate().
    listening = threading.Event()
    ports: list[int] = []
    stderr_lines: list[str] = []

    def read_logs() -> None:
        with stderr_pipe:
            for line in stderr_pipe:
                stderr_lines.append(line)
                if match := re.search(r"serve_http listening on port (\d+)", line):
                    ports.append(int(match.group(1)))
                    listening.set()

    log_reader = threading.Thread(target=read_logs, daemon=True)
    log_reader.start()
    try:
        port = _bound_port(process, listening, ports)
    except Exception as error:
        _terminate(process)
        stdout, _ = process.communicate()
        log_reader.join(timeout=5)
        error.add_note(
            f"tenzir exit code: {process.returncode}\nstdout: {stdout}"
            f"\nstderr: {''.join(stderr_lines)}"
        )
        raise
    response_ready = threading.Event()
    payload_received = [threading.Event() for _ in lines]
    response: list[tuple[int, str]] = []
    request_errors: list[Exception] = []
    response_errors: list[Exception] = []

    def request() -> None:
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            connection = http.client.HTTPConnection("127.0.0.1", port, timeout=20)
            try:
                connection.request("GET", "/")
                result = connection.getresponse()
                response_ready.set()
                body = bytearray()
                while chunk := result.read1(4096):
                    body.extend(chunk)
                    for prefix, received in zip(
                        prefixes, payload_received, strict=True
                    ):
                        if prefix in body:
                            received.set()
                    if payload_received[-1].is_set():
                        response.append((result.status, body.decode()))
                        return
                response.append((result.status, body.decode()))
                return
            except (OSError, http.client.HTTPException) as error:
                if response_ready.is_set():
                    response_errors.append(error)
                    return
                request_errors.append(error)
                time.sleep(0.1)
            finally:
                connection.close()

    worker = threading.Thread(target=request, daemon=True)
    worker.start()
    try:
        deadline = time.monotonic() + 30
        while not response_ready.wait(0.25):
            if process.poll() is not None:
                break
            _write(process, "ready")
            if time.monotonic() >= deadline:
                break
        if not response_ready.is_set():
            raise AssertionError(
                f"HTTP client did not register: {request_errors[-1:]!r}"
            )
        for line, received in zip(lines, payload_received, strict=True):
            _write(process, line)
            if not received.wait(30):
                raise AssertionError("HTTP client did not receive the expected payload")
        assert process.stdin is not None
        process.stdin.close()
        process.stdin = None
        stdout, _ = process.communicate(timeout=30)
        log_reader.join(timeout=5)
        worker.join(timeout=5)
        assert process.returncode == 0, (process.returncode, stdout, stderr_lines)
        assert not worker.is_alive(), (request_errors, response)
        assert not response_errors, response_errors
        assert len(response) == 1, response
        status, body = response[0]
        assert status == 200, response
        received = body.splitlines(keepends=True)
        assert len(received) > len(lines), response
        assert received[: -len(lines)] == ["ready\n"] * (len(received) - len(lines))
        assert received[-len(lines) :] == [f"{line}\n" for line in lines]
    except Exception as error:
        _terminate(process)
        stdout, _ = process.communicate()
        log_reader.join(timeout=5)
        error.add_note(
            f"tenzir exit code: {process.returncode}\nstdout: {stdout}"
            f"\nstderr: {''.join(stderr_lines)}"
        )
        raise
    finally:
        _terminate(process)


def main() -> None:
    _run_case(["hello-from-serve_http"])
    _run_case(["first-line", "second-line"])
    print("ok")


if __name__ == "__main__":
    main()
