from __future__ import annotations

import shutil
import socket
import ssl
import tempfile
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from tenzir_test import FixtureHandle, fixture
from tenzir_test.fixtures import current_options

from ._utils import find_free_port, generate_self_signed_cert

_HOST = "127.0.0.1"
_COMMON_NAME = "tenzir-node.example.org"
_CLIENT_RETRY_DELAY = 0.01
_ASSERTION_TIMEOUT = 2.0


@dataclass(frozen=True)
class RelpOptions:
    payloads: list[str] | None = None
    coalesce: bool = False
    fragment_size: int = 0
    tls: bool = False
    certs: bool = False
    client_cert: bool = False
    unauthenticated_first: bool = False
    relp_version: int | str = 1
    invalid_first: str | None = None
    invalid_utf8: bool = False


@dataclass(frozen=True)
class RelpAssertions:
    response_count: int | None = None
    responses_contain: str | None = None
    initial_responses_contain: str | None = None
    unauthenticated_response_count: int | None = None


@dataclass
class _RelpState:
    lock: threading.Lock = field(default_factory=threading.Lock)
    responses: list[bytes] = field(default_factory=list)
    initial_responses: list[bytes] = field(default_factory=list)
    unauthenticated_responses: list[bytes] = field(default_factory=list)


def _frame(transaction_id: int, command: str, payload: bytes = b"") -> bytes:
    header = f"{transaction_id} {command} {len(payload)}".encode()
    return header + (b" " + payload if payload else b"") + b"\n"


def _recv_exact(sock: socket.socket, size: int) -> bytes:
    result = bytearray()
    while len(result) < size:
        chunk = sock.recv(size - len(result))
        if not chunk:
            raise EOFError("connection closed inside RELP frame")
        result.extend(chunk)
    return bytes(result)


def _recv_field(sock: socket.socket) -> tuple[bytes, bytes]:
    result = bytearray()
    while True:
        octet = _recv_exact(sock, 1)
        if octet in {b" ", b"\n"}:
            return bytes(result), octet
        result.extend(octet)


def _recv_frame(sock: socket.socket) -> bytes:
    transaction_id, delimiter = _recv_field(sock)
    if delimiter != b" ":
        raise RuntimeError("invalid RELP response transaction number")
    command, delimiter = _recv_field(sock)
    if delimiter != b" ":
        raise RuntimeError("invalid RELP response command")
    length_text, delimiter = _recv_field(sock)
    length = int(length_text)
    if length == 0:
        if delimiter != b"\n":
            raise RuntimeError("invalid empty RELP response")
        return transaction_id + b" " + command + b" 0\n"
    if delimiter != b" ":
        raise RuntimeError("invalid RELP response payload delimiter")
    payload = _recv_exact(sock, length)
    if _recv_exact(sock, 1) != b"\n":
        raise RuntimeError("invalid RELP response trailer")
    return transaction_id + b" " + command + b" " + length_text + b" " + payload + b"\n"


def _send(sock: socket.socket, data: bytes, fragment_size: int) -> None:
    if fragment_size <= 0:
        sock.sendall(data)
        return
    for offset in range(0, len(data), fragment_size):
        sock.sendall(data[offset : offset + fragment_size])
        time.sleep(0.001)


def _connect(port: int, stop_event: threading.Event) -> socket.socket | None:
    while not stop_event.is_set():
        try:
            return socket.create_connection((_HOST, port), timeout=0.2)
        except OSError:
            stop_event.wait(_CLIENT_RETRY_DELAY)
    return None


def _run_session(
    *,
    port: int,
    stop_event: threading.Event,
    state: _RelpState,
    response_target: list[bytes],
    payloads: list[bytes],
    tls: bool,
    client_cert: bool,
    ca_path: Path | None,
    cert_path: Path | None,
    key_path: Path | None,
    relp_version: int | str = 1,
    coalesce: bool = False,
    fragment_size: int = 0,
) -> None:
    raw_socket = _connect(port, stop_event)
    if raw_socket is None:
        return
    try:
        with raw_socket:
            raw_socket.settimeout(5)
            sock: socket.socket
            if tls:
                assert ca_path is not None
                context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
                context.check_hostname = False
                context.load_verify_locations(cafile=ca_path)
                if client_cert:
                    assert cert_path is not None
                    assert key_path is not None
                    context.load_cert_chain(certfile=cert_path, keyfile=key_path)
                sock = context.wrap_socket(raw_socket, server_hostname=_COMMON_NAME)
            else:
                sock = raw_socket
            with sock:
                sock.settimeout(5)
                open_payload = f"relp_version={relp_version}\ncommands=syslog".encode()
                frames = [_frame(1, "open", open_payload)]
                frames.extend(
                    _frame(transaction_id, "syslog", payload)
                    for transaction_id, payload in enumerate(payloads, start=2)
                )
                frames.append(_frame(len(payloads) + 2, "close"))
                try:
                    if coalesce:
                        _send(sock, b"".join(frames), fragment_size)
                    else:
                        for frame in frames:
                            _send(sock, frame, fragment_size)
                except OSError:
                    # A finite test pipeline may stop after the last syslog
                    # event before the fixture finishes sending `close`.
                    pass
                for _ in frames:
                    try:
                        response = _recv_frame(sock)
                    except (EOFError, OSError, RuntimeError):
                        break
                    with state.lock:
                        response_target.append(response)
    except (EOFError, OSError, RuntimeError, ssl.SSLError):
        return


def _run_client(
    *,
    port: int,
    stop_event: threading.Event,
    state: _RelpState,
    options: RelpOptions,
    ca_path: Path | None,
    cert_path: Path | None,
    key_path: Path | None,
) -> None:
    if options.invalid_first is not None:
        invalid_frames = {
            "malformed": b"x open 0\n",
            "oversized": b"1 open 999999999 ",
            "overflowing_version": _frame(
                1, "open", b"relp_version=4294967296\ncommands=syslog"
            ),
            "unexpected_transaction": _frame(
                2, "open", b"relp_version=0\ncommands=syslog"
            ),
        }
        try:
            invalid_frame = invalid_frames[options.invalid_first]
        except KeyError as exc:
            raise RuntimeError(
                "relp fixture `invalid_first` must be one of: malformed, "
                "overflowing_version, oversized, unexpected_transaction"
            ) from exc
        raw_socket = _connect(port, stop_event)
        if raw_socket is None:
            return
        with raw_socket:
            raw_socket.sendall(invalid_frame)
            raw_socket.settimeout(2)
            while True:
                try:
                    response = _recv_frame(raw_socket)
                except (EOFError, OSError, RuntimeError):
                    break
                with state.lock:
                    state.initial_responses.append(response)
    if options.unauthenticated_first:
        _run_session(
            port=port,
            stop_event=stop_event,
            state=state,
            response_target=state.unauthenticated_responses,
            payloads=[b"unauthenticated"],
            tls=options.tls,
            client_cert=False,
            ca_path=ca_path,
            cert_path=cert_path,
            key_path=key_path,
        )
    payloads = options.payloads or ["hello\nworld", "without trailing newline"]
    payload_bytes = [payload.encode() for payload in payloads]
    if options.invalid_utf8:
        payload_bytes.insert(0, b"\xff")
    _run_session(
        port=port,
        stop_event=stop_event,
        state=state,
        response_target=state.responses,
        payloads=payload_bytes,
        tls=options.tls,
        client_cert=options.client_cert,
        ca_path=ca_path,
        cert_path=cert_path,
        key_path=key_path,
        relp_version=options.relp_version,
        coalesce=options.coalesce,
        fragment_size=options.fragment_size,
    )


@fixture(options=RelpOptions, assertions=RelpAssertions)
def relp() -> FixtureHandle:
    options = current_options("relp")
    if options.fragment_size < 0:
        raise RuntimeError("relp fixture `fragment_size` must not be negative")
    port = find_free_port()
    endpoint = f"{_HOST}:{port}"
    stop_event = threading.Event()
    state = _RelpState()
    temp_dir = (
        Path(tempfile.mkdtemp(prefix="relp-")) if options.tls or options.certs else None
    )
    cert_path: Path | None = None
    key_path: Path | None = None
    ca_path: Path | None = None
    cert_and_key_path: Path | None = None
    if temp_dir is not None:
        cert_path, key_path, ca_path, cert_and_key_path = generate_self_signed_cert(
            temp_dir, _COMMON_NAME
        )
    worker = threading.Thread(
        target=_run_client,
        kwargs={
            "port": port,
            "stop_event": stop_event,
            "state": state,
            "options": options,
            "ca_path": ca_path,
            "cert_path": cert_path,
            "key_path": key_path,
        },
        daemon=True,
    )
    worker.start()
    env = {"RELP_ENDPOINT": endpoint}
    if temp_dir is not None:
        assert cert_path is not None
        assert key_path is not None
        assert ca_path is not None
        assert cert_and_key_path is not None
        env.update(
            {
                "RELP_CERTFILE": str(cert_path),
                "RELP_KEYFILE": str(key_path),
                "RELP_CAFILE": str(ca_path),
                "RELP_CERTKEYFILE": str(cert_and_key_path),
            }
        )

    def _assert_test(
        *, test: Path, assertions: RelpAssertions | dict[str, Any], **_: Any
    ) -> None:
        if isinstance(assertions, dict):
            assertions = RelpAssertions(**assertions)
        deadline = time.monotonic() + _ASSERTION_TIMEOUT
        while True:
            with state.lock:
                responses = list(state.responses)
                initial_responses = list(state.initial_responses)
                unauthenticated_responses = list(state.unauthenticated_responses)
            rendered = b"".join(responses).decode(errors="replace")
            rendered_initial = b"".join(initial_responses).decode(errors="replace")
            error = None
            if (
                assertions.response_count is not None
                and len(responses) != assertions.response_count
            ):
                error = (
                    f"{test.name}: expected {assertions.response_count} RELP "
                    f"responses, got {len(responses)}"
                )
            elif (
                assertions.responses_contain is not None
                and assertions.responses_contain not in rendered
            ):
                error = (
                    f"{test.name}: expected RELP responses to contain "
                    f"{assertions.responses_contain!r}, got {rendered!r}"
                )
            elif (
                assertions.initial_responses_contain is not None
                and assertions.initial_responses_contain not in rendered_initial
            ):
                error = (
                    f"{test.name}: expected initial RELP responses to contain "
                    f"{assertions.initial_responses_contain!r}, got "
                    f"{rendered_initial!r}"
                )
            elif (
                assertions.unauthenticated_response_count is not None
                and len(unauthenticated_responses)
                != assertions.unauthenticated_response_count
            ):
                error = (
                    f"{test.name}: expected "
                    f"{assertions.unauthenticated_response_count} unauthenticated "
                    f"RELP responses, got {len(unauthenticated_responses)}"
                )
            if error is None:
                return
            if time.monotonic() >= deadline:
                raise AssertionError(error)
            time.sleep(0.01)

    def _teardown() -> None:
        stop_event.set()
        worker.join(timeout=6)
        if worker.is_alive():
            raise RuntimeError("relp fixture worker did not stop within 6 seconds")
        if temp_dir is not None:
            shutil.rmtree(temp_dir, ignore_errors=True)

    return FixtureHandle(
        env=env,
        teardown=_teardown,
        hooks={"assert_test": _assert_test},
    )
