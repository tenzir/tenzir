from __future__ import annotations

import socket
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path

from tenzir_test import FixtureHandle, fixture
from tenzir_test.fixtures import current_options

from ._utils import find_free_port

_HOST = "127.0.0.1"
_ASSERTION_WAIT_INTERVAL = 0.01
_ASSERTION_WAIT_TIMEOUT = 2.0
_DOWNLOAD_PATH = "/download.ndjson"
_UPLOAD_PATH = "/upload.ndjson"


@dataclass(frozen=True)
class FtpOptions:
    download_payload: str = '{"message":"hello-from-from_ftp"}\n'
    download_chunk_size: int | None = None
    download_chunk_delay: float = 0.0


@dataclass(frozen=True)
class FtpAssertions:
    uploaded_contains: str | None = None


@dataclass
class _FtpState:
    lock: threading.Lock = field(default_factory=threading.Lock)
    uploaded: bytearray = field(default_factory=bytearray)


def _normalize_path(path: str) -> str:
    if not path:
        return "/"
    return "/" + path.lstrip("/")


def _send_line(control: socket.socket, line: str) -> None:
    control.sendall(f"{line}\r\n".encode("ascii"))


def _recv_line(control_file) -> str | None:
    line = control_file.readline()
    if not line:
        return None
    return line.decode("utf-8", errors="replace").rstrip("\r\n")


def _open_passive_listener() -> tuple[socket.socket, int]:
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind((_HOST, 0))
    listener.listen(1)
    return listener, listener.getsockname()[1]


def _handle_client(
    control: socket.socket,
    state: _FtpState,
    download_payload: bytes,
    download_chunk_size: int | None,
    download_chunk_delay: float,
) -> None:
    control.settimeout(1.0)
    control_file = control.makefile("rb")
    passive_listener: socket.socket | None = None
    _send_line(control, "220 ftp fixture ready")
    try:
        while True:
            line = _recv_line(control_file)
            if line is None:
                return
            command, _, argument = line.partition(" ")
            command = command.upper()
            argument = argument.strip()
            if command in {"USER", "PASS", "ACCT"}:
                _send_line(control, "230 logged in")
                continue
            if command in {"TYPE", "MODE", "STRU", "OPTS", "NOOP", "CLNT"}:
                _send_line(control, "200 ok")
                continue
            if command == "FEAT":
                _send_line(control, "211 no-features")
                continue
            if command == "SYST":
                _send_line(control, "215 UNIX Type: L8")
                continue
            if command == "PWD":
                _send_line(control, '257 "/" is the current directory')
                continue
            if command in {"CWD", "CDUP"}:
                _send_line(control, "250 directory changed")
                continue
            if command == "SIZE":
                path = _normalize_path(argument)
                if path == _DOWNLOAD_PATH:
                    _send_line(control, f"213 {len(download_payload)}")
                else:
                    _send_line(control, "550 file unavailable")
                continue
            if command == "REST":
                _send_line(control, "350 restarting at requested offset")
                continue
            if command in {"EPSV", "PASV"}:
                if passive_listener is not None:
                    passive_listener.close()
                passive_listener, passive_port = _open_passive_listener()
                if command == "EPSV":
                    _send_line(
                        control,
                        f"229 Entering Extended Passive Mode (|||{passive_port}|)",
                    )
                else:
                    p1, p2 = divmod(passive_port, 256)
                    host = _HOST.split(".")
                    _send_line(
                        control,
                        "227 Entering Passive Mode "
                        f"({host[0]},{host[1]},{host[2]},{host[3]},{p1},{p2})",
                    )
                continue
            if command == "RETR":
                if passive_listener is None:
                    _send_line(control, "425 use PASV or EPSV first")
                    continue
                path = _normalize_path(argument)
                if path != _DOWNLOAD_PATH:
                    _send_line(control, "550 file unavailable")
                    passive_listener.close()
                    passive_listener = None
                    continue
                _send_line(control, "150 opening data connection")
                passive_listener.settimeout(1.0)
                data_conn, _ = passive_listener.accept()
                passive_listener.close()
                passive_listener = None
                with data_conn:
                    if download_chunk_size is None or download_chunk_size <= 0:
                        data_conn.sendall(download_payload)
                    else:
                        for i in range(0, len(download_payload), download_chunk_size):
                            data_conn.sendall(
                                download_payload[i : i + download_chunk_size]
                            )
                            if (
                                download_chunk_delay > 0
                                and i + download_chunk_size < len(download_payload)
                            ):
                                time.sleep(download_chunk_delay)
                _send_line(control, "226 transfer complete")
                continue
            if command == "STOR":
                if passive_listener is None:
                    _send_line(control, "425 use PASV or EPSV first")
                    continue
                _send_line(control, "150 opening data connection")
                passive_listener.settimeout(1.0)
                data_conn, _ = passive_listener.accept()
                passive_listener.close()
                passive_listener = None
                received = bytearray()
                with data_conn:
                    while True:
                        chunk = data_conn.recv(65536)
                        if not chunk:
                            break
                        received.extend(chunk)
                with state.lock:
                    state.uploaded = received
                _send_line(control, "226 transfer complete")
                continue
            if command == "QUIT":
                _send_line(control, "221 goodbye")
                return
            _send_line(control, "502 command not implemented")
    finally:
        if passive_listener is not None:
            passive_listener.close()
        control_file.close()


def _run_server(
    stop_event: threading.Event,
    state: _FtpState,
    payload: str,
    port: int,
    download_chunk_size: int | None,
    download_chunk_delay: float,
) -> None:
    download_payload = payload.encode("utf-8")
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((_HOST, port))
        server.listen(5)
        server.settimeout(0.1)
        while not stop_event.is_set():
            try:
                control, _ = server.accept()
            except TimeoutError:
                continue
            with control:
                _handle_client(
                    control,
                    state,
                    download_payload,
                    download_chunk_size,
                    download_chunk_delay,
                )


@fixture(options=FtpOptions, assertions=FtpAssertions)
def ftp() -> FixtureHandle:
    opts = current_options("ftp")
    port = find_free_port()
    state = _FtpState()
    stop_event = threading.Event()
    worker = threading.Thread(
        target=_run_server,
        kwargs={
            "stop_event": stop_event,
            "state": state,
            "payload": opts.download_payload,
            "port": port,
            "download_chunk_size": opts.download_chunk_size,
            "download_chunk_delay": opts.download_chunk_delay,
        },
        daemon=True,
    )
    worker.start()
    base_url = f"ftp://{_HOST}:{port}"
    env = {
        "FTP_DOWNLOAD_URL": f"{base_url}{_DOWNLOAD_PATH}",
        "FTP_DOWNLOAD_URL_NO_SCHEME": f"{_HOST}:{port}{_DOWNLOAD_PATH}",
        "FTP_UPLOAD_URL": f"{base_url}{_UPLOAD_PATH}",
    }

    def _assert_test(
        *, test: Path, assertions: FtpAssertions | dict[str, object], **_: object
    ) -> None:
        if isinstance(assertions, dict):
            assertions = FtpAssertions(**assertions)
        deadline = time.monotonic() + _ASSERTION_WAIT_TIMEOUT
        while True:
            with state.lock:
                uploaded = state.uploaded.decode("utf-8", errors="replace")
            if (
                assertions.uploaded_contains is None
                or assertions.uploaded_contains in uploaded
            ):
                return
            if time.monotonic() >= deadline:
                raise AssertionError(
                    f"{test.name}: expected uploaded data to contain "
                    f"{assertions.uploaded_contains!r}, got {uploaded!r}"
                )
            time.sleep(_ASSERTION_WAIT_INTERVAL)

    def _teardown() -> None:
        stop_event.set()
        worker.join(timeout=1.0)
        if worker.is_alive():
            raise RuntimeError("failed to stop ftp fixture worker")

    return FixtureHandle(
        env=env,
        teardown=_teardown,
        hooks={"assert_test": _assert_test},
    )
