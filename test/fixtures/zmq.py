from __future__ import annotations

import importlib.util
import os
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from tenzir_test import FixtureHandle, fixture
from tenzir_test.fixtures import FixtureUnavailable, current_options

from ._utils import find_free_port

_HOST = "127.0.0.1"
_STARTUP_TIMEOUT = 20.0
_STARTUP_POLL_INTERVAL = 0.05
_ASSERTION_WAIT_TIMEOUT = 2.0
_ASSERTION_WAIT_INTERVAL = 0.01
_HELPER = Path(__file__).resolve().parent / "tools" / "zmq_peer.py"


@dataclass(frozen=True)
class ZmqOptions:
    role: str = "publisher"  # publisher sends payload, subscriber receives
    mode: str = "bind"  # bind or connect
    payload: str = '{"line":"foo"}'
    subscribe_prefix: str = ""
    send_interval_ms: int = 50
    # Feed NDJSON over a FIFO until the subscriber observes an outgoing message.
    input_until_received: bool = False


@dataclass(frozen=True)
class ZmqAssertions:
    received_contains: str | None = None
    # Exact set of complete captured lines; repeated deliveries are allowed.
    received_lines: list[str] | None = None


@fixture(options=ZmqOptions, assertions=ZmqAssertions)
def zmq() -> FixtureHandle:
    if importlib.util.find_spec("zmq") is None:
        raise FixtureUnavailable("pyzmq package not installed")
    opts = current_options("zmq")
    if opts.role not in {"publisher", "subscriber"}:
        raise RuntimeError(
            "zmq fixture option `role` must be one of: publisher, subscriber"
        )
    if opts.mode not in {"bind", "connect"}:
        raise RuntimeError("zmq fixture option `mode` must be one of: bind, connect")
    if opts.send_interval_ms <= 0:
        raise RuntimeError("zmq fixture option `send_interval_ms` must be > 0")
    if opts.input_until_received and opts.role != "subscriber":
        raise RuntimeError("zmq fixture input requires a subscriber")
    input_path = None
    if opts.input_until_received:
        input_fd, input_path = tempfile.mkstemp(prefix="zmq-input-", suffix=".fifo")
        os.close(input_fd)
        os.unlink(input_path)
        os.mkfifo(input_path)
    endpoint = f"tcp://{_HOST}:{find_free_port()}"
    ready_fd, ready_path = tempfile.mkstemp(prefix="zmq-ready-", suffix=".flag")
    os.close(ready_fd)
    os.unlink(ready_path)
    capture_fd, capture_path = tempfile.mkstemp(prefix="zmq-capture-", suffix=".log")
    os.close(capture_fd)
    helper_log_fd, helper_log_path = tempfile.mkstemp(
        prefix="zmq-helper-", suffix=".log"
    )
    os.close(helper_log_fd)
    helper_stdout = open(helper_log_path, "wb")
    cmd = [
        sys.executable,
        str(_HELPER),
        "--role",
        opts.role,
        "--mode",
        opts.mode,
        "--endpoint",
        endpoint,
        "--ready-file",
        ready_path,
        "--capture-file",
        capture_path,
        "--send-interval-ms",
        str(opts.send_interval_ms),
        "--payload",
        opts.payload,
        "--subscribe-prefix",
        opts.subscribe_prefix,
    ]
    if input_path is not None:
        cmd.extend(["--input-fifo", input_path])
    proc = subprocess.Popen(
        cmd,
        stdout=helper_stdout,
        stderr=subprocess.STDOUT,
        close_fds=True,
    )
    deadline = time.monotonic() + _STARTUP_TIMEOUT
    while not os.path.exists(ready_path):
        if proc.poll() is not None:
            helper_stdout.close()
            log = Path(helper_log_path).read_text(errors="replace")
            raise RuntimeError(
                f"zmq fixture helper exited with code {proc.returncode}: {log}"
            )
        if time.monotonic() >= deadline:
            proc.terminate()
            proc.wait(timeout=2)
            helper_stdout.close()
            log = Path(helper_log_path).read_text(errors="replace")
            raise RuntimeError(f"zmq fixture helper did not become ready: {log}")
        time.sleep(_STARTUP_POLL_INTERVAL)

    env = {
        "ZMQ_ENDPOINT": endpoint,
        "ZMQ_FILE": capture_path,
    }
    if input_path is not None:
        env["ZMQ_INPUT_FIFO"] = input_path

    def _helper_diagnostics() -> str:
        status = "is still running"
        if proc.poll() is not None:
            status = f"exited with code {proc.returncode}"
        log = Path(helper_log_path).read_text(errors="replace")
        return f"helper {status}; helper log: {log!r}"

    def _assert_test(
        *,
        test: Path,
        assertions: ZmqAssertions | dict[str, Any],
        **_: Any,
    ) -> None:
        if isinstance(assertions, dict):
            assertions = ZmqAssertions(**assertions)
        if assertions.received_contains is None and assertions.received_lines is None:
            return
        expected_lines = (
            set(assertions.received_lines)
            if assertions.received_lines is not None
            else None
        )
        deadline = time.monotonic() + _ASSERTION_WAIT_TIMEOUT
        while True:
            received = Path(capture_path).read_text(errors="replace")
            # Ignore an incomplete trailing line while the helper is appending.
            lines = set(received.rpartition("\n")[0].splitlines())
            if expected_lines is not None and lines - expected_lines:
                raise AssertionError(
                    f"{test.name}: unexpected fixture capture lines "
                    f"{sorted(lines - expected_lines)!r}; {_helper_diagnostics()}"
                )
            contains_matches = (
                assertions.received_contains is None
                or assertions.received_contains in received
            )
            lines_match = expected_lines is None or lines == expected_lines
            if contains_matches and lines_match:
                return
            if proc.poll() is not None or time.monotonic() >= deadline:
                raise AssertionError(
                    f"{test.name}: expected fixture capture to contain "
                    f"{assertions.received_contains!r} with lines "
                    f"{assertions.received_lines!r}, got {received!r}; "
                    f"{_helper_diagnostics()}"
                )
            time.sleep(_ASSERTION_WAIT_INTERVAL)

    def _teardown() -> None:
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=5)
        helper_stdout.close()
        for path in (ready_path, capture_path, helper_log_path, input_path):
            if path is not None and os.path.exists(path):
                os.remove(path)

    return FixtureHandle(
        env=env,
        teardown=_teardown,
        hooks={"assert_test": _assert_test},
    )
