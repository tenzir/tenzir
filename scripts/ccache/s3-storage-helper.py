#!/usr/bin/env python3
"""ccache remote storage helper backed by S3."""

from __future__ import annotations

import argparse
import collections
import contextlib
import errno
import json
import logging
import os
import queue
import re
import select
import shutil
import signal
import socket
import socketserver
import stat
import subprocess
import sys
import termios
import threading
import time
import tty
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import BinaryIO, Callable, Iterator, NoReturn, Protocol
from urllib.parse import unquote, urlparse
from urllib.request import Request, urlopen


PROTOCOL_VERSION = 0x01
CAP_GET_PUT_REMOVE_STOP = 0x00

REQUEST_GET = 0x00
REQUEST_PUT = 0x01
REQUEST_REMOVE = 0x02
REQUEST_STOP = 0x03

RESPONSE_OK = 0x00
RESPONSE_NOOP = 0x01
RESPONSE_ERR = 0x02

PUT_OVERWRITE = 0x01

DEFAULT_REGION = "eu-central-1"
DEFAULT_INFRASTRUCTURE_ACCOUNT_ID = "622024652768"
GITHUB_OIDC_PROVIDER_URL = "https://token.actions.githubusercontent.com"
GITHUB_OIDC_PROVIDER_HOST = "token.actions.githubusercontent.com"
GITHUB_OIDC_AUDIENCE = "sts.amazonaws.com"
PUBLIC_ACCESS_BLOCK_CONFIGURATION = {
    "BlockPublicAcls": True,
    "IgnorePublicAcls": True,
    "BlockPublicPolicy": True,
    "RestrictPublicBuckets": True,
}


class Storage(Protocol):
    def get(self, key: bytes) -> bytes | None: ...

    def put(self, key: bytes, value: bytes, overwrite: bool) -> bool: ...

    def remove(self, key: bytes) -> bool: ...

    def close(self) -> None: ...


class StorageError(Exception):
    """Short, user-facing storage error."""


@dataclass(frozen=True)
class S3Config:
    bucket: str
    prefix: str
    region: str | None
    endpoint_url: str | None
    profile: str | None
    auth: str
    expected_account_id: str | None
    access_key_id: str | None
    secret_access_key: str | None
    session_token: str | None
    credential_expiration: str | None
    oidc_broker_url: str | None
    oidc_audience: str
    layout: str
    max_pool_connections: int
    object_list_min_interval: float
    upload_queue_size: int
    upload_workers: int
    upload_drain_timeout: float


@dataclass(frozen=True)
class UploadTask:
    object_key: str
    value: bytes


UploadQueueItem = UploadTask | None


@dataclass(frozen=True)
class SetupConfig:
    bucket: str
    prefix: str
    region: str
    role_name: str
    policy_name: str
    github_subject: str
    yes: bool


class S3Storage:
    def __init__(self, config: S3Config) -> None:
        try:
            import boto3
            from botocore.config import Config
            from botocore.credentials import RefreshableCredentials
            from botocore.exceptions import BotoCoreError, ClientError
        except ImportError as error:
            raise StorageError("boto3 is required for S3 ccache storage") from error

        self._config = config
        self._client_error = ClientError
        self._boto_core_error = BotoCoreError

        session_args = {}
        if config.profile:
            session_args["profile_name"] = config.profile
        if config.region:
            session_args["region_name"] = config.region
        session = boto3.Session(**session_args)
        if config.auth == "cloudflare-r2-oidc-broker":
            if not config.credential_expiration:
                raise StorageError("R2 broker credentials are missing an expiration time")
            refreshable_credentials = RefreshableCredentials.create_from_metadata(
                metadata={
                    "access_key": config.access_key_id,
                    "secret_key": config.secret_access_key,
                    "token": config.session_token,
                    "expiry_time": config.credential_expiration,
                },
                refresh_using=lambda: _cloudflare_r2_oidc_broker_metadata(config),
                method="cloudflare-r2-oidc-broker",
            )
            session._session._credentials = refreshable_credentials
            session._session.set_config_variable("region", config.region or "auto")
        credentials = session.get_credentials()
        if credentials is None:
            raise StorageError(
                "AWS credentials not found; set AWS_PROFILE or provide credentials "
                "via the AWS SDK credential chain",
            )
        try:
            credentials.get_frozen_credentials()
        except BotoCoreError as error:
            raise StorageError(f"could not resolve AWS credentials: {error}") from error

        client_args = {
            "config": Config(max_pool_connections=config.max_pool_connections),
        }
        if config.access_key_id:
            client_args["aws_access_key_id"] = config.access_key_id
        if config.secret_access_key:
            client_args["aws_secret_access_key"] = config.secret_access_key
        if config.session_token:
            client_args["aws_session_token"] = config.session_token
        if config.endpoint_url:
            client_args["endpoint_url"] = config.endpoint_url
        self._s3 = session.client("s3", **client_args)
        self._known_objects: set[str] = set()
        self._known_objects_lock = threading.Lock()
        self._last_object_list_refresh = float("-inf")
        self._queued_objects: set[str] = set()
        self._queued_objects_lock = threading.Lock()
        self._upload_queue: queue.Queue[UploadQueueItem] = queue.Queue(
            maxsize=config.upload_queue_size,
        )
        self._upload_workers = [
            threading.Thread(
                target=self._upload_worker,
                name=f"s3-upload-{index}",
                daemon=True,
            )
            for index in range(config.upload_workers)
        ]
        for worker in self._upload_workers:
            worker.start()

    def status(self) -> str:
        return (
            f"queue {self._upload_queue.qsize()}/{self._config.upload_queue_size} "
            f"known {self._known_object_count()} "
            f"bucket {self._config.bucket}/{self._config.prefix or '-'}"
        )

    def get(self, key: bytes) -> bytes | None:
        object_key = self._object_key(key)
        try:
            response = self._s3.get_object(
                Bucket=self._config.bucket,
                Key=object_key,
            )
            value = response["Body"].read()
            self._remember_object(object_key)
            logging.info("cache hit object=%s size=%d", object_key, len(value))
            return value
        except self._client_error as error:
            if _is_not_found(error):
                logging.info("cache miss object=%s", object_key)
                return None
            logging.warning("cache get failed object=%s error=%s", object_key, error)
            raise StorageError(_client_error_message(error)) from error
        except self._boto_core_error as error:
            logging.warning("cache get failed object=%s error=%s", object_key, error)
            raise StorageError(str(error)) from error

    def put(self, key: bytes, value: bytes, overwrite: bool) -> bool:
        object_key = self._object_key(key)
        if not overwrite:
            if self._known_object_exists(object_key):
                logging.info(
                    "cache put skipped object=%s reason=known-exists",
                    object_key,
                )
                return False
            if self._mark_object_queued(object_key):
                self._upload_queue.put(UploadTask(object_key, value))
                logging.info(
                    "cache put queued object=%s size=%d queue=%d/%d",
                    object_key,
                    len(value),
                    self._upload_queue.qsize(),
                    self._config.upload_queue_size,
                )
                return True
            logging.info("cache put skipped object=%s reason=queued", object_key)
            return False
        return self._put_object(object_key, value, overwrite=True)

    def remove(self, key: bytes) -> bool:
        object_key = self._object_key(key)
        try:
            if not self._exists_object(object_key):
                logging.info(
                    "cache remove skipped object=%s reason=missing", object_key
                )
                return False
            self._s3.delete_object(
                Bucket=self._config.bucket,
                Key=object_key,
            )
        except self._client_error as error:
            logging.warning("cache remove failed object=%s error=%s", object_key, error)
            raise StorageError(_client_error_message(error)) from error
        except self._boto_core_error as error:
            logging.warning("cache remove failed object=%s error=%s", object_key, error)
            raise StorageError(str(error)) from error
        self._forget_object(object_key)
        logging.info("cache remove object=%s", object_key)
        return True

    def close(self) -> None:
        deadline = time.monotonic() + self._config.upload_drain_timeout
        while self._upload_queue.unfinished_tasks and time.monotonic() < deadline:
            time.sleep(0.05)
        unfinished = self._upload_queue.unfinished_tasks
        if unfinished:
            logging.warning("cache upload queue not drained pending=%d", unfinished)
        for _worker in self._upload_workers:
            try:
                self._upload_queue.put(None, timeout=1)
            except queue.Full:
                logging.warning(
                    "could not stop cache upload worker because queue is full"
                )
        join_timeout = max(0.0, deadline - time.monotonic())
        for worker in self._upload_workers:
            worker.join(timeout=join_timeout)

    def _upload_worker(self) -> None:
        while True:
            task = self._upload_queue.get()
            try:
                if task is None:
                    return
                self._put_object(task.object_key, task.value, overwrite=False)
            except Exception as error:
                logging.warning(
                    "cache put failed object=%s error=%s",
                    task.object_key if task is not None else "<stop>",
                    error,
                )
            finally:
                if task is not None:
                    self._unmark_object_queued(task.object_key)
                    logging.info(
                        "cache put dequeued object=%s queue=%d/%d",
                        task.object_key,
                        self._upload_queue.qsize(),
                        self._config.upload_queue_size,
                    )
                self._upload_queue.task_done()

    def _put_object(self, object_key: str, value: bytes, overwrite: bool) -> bool:
        try:
            if not overwrite and self._known_object_exists(object_key):
                logging.info(
                    "cache put skipped object=%s reason=known-exists",
                    object_key,
                )
                return False
            if not overwrite and self._exists_object(object_key):
                self._refresh_known_objects_debounced()
                logging.info("cache put skipped object=%s reason=exists", object_key)
                return False
            self._s3.put_object(
                Bucket=self._config.bucket,
                Key=object_key,
                Body=value,
            )
        except self._client_error as error:
            logging.warning("cache put failed object=%s error=%s", object_key, error)
            raise StorageError(_client_error_message(error)) from error
        except self._boto_core_error as error:
            logging.warning("cache put failed object=%s error=%s", object_key, error)
            raise StorageError(str(error)) from error
        self._remember_object(object_key)
        logging.info("cache put object=%s size=%d", object_key, len(value))
        return True

    def _exists_object(self, object_key: str) -> bool:
        try:
            self._s3.head_object(
                Bucket=self._config.bucket,
                Key=object_key,
            )
        except self._client_error as error:
            if _is_not_found(error):
                return False
            raise StorageError(_client_error_message(error)) from error
        except self._boto_core_error as error:
            raise StorageError(str(error)) from error
        return True

    def _known_object_exists(self, object_key: str) -> bool:
        with self._known_objects_lock:
            return object_key in self._known_objects

    def _remember_object(self, object_key: str) -> None:
        with self._known_objects_lock:
            self._known_objects.add(object_key)

    def _forget_object(self, object_key: str) -> None:
        with self._known_objects_lock:
            self._known_objects.discard(object_key)

    def _known_object_count(self) -> int:
        with self._known_objects_lock:
            return len(self._known_objects)

    def _mark_object_queued(self, object_key: str) -> bool:
        with self._queued_objects_lock:
            if object_key in self._queued_objects:
                return False
            self._queued_objects.add(object_key)
            return True

    def _unmark_object_queued(self, object_key: str) -> None:
        with self._queued_objects_lock:
            self._queued_objects.discard(object_key)

    def _refresh_known_objects_debounced(self) -> None:
        now = time.monotonic()
        with self._known_objects_lock:
            if (
                now - self._last_object_list_refresh
                < self._config.object_list_min_interval
            ):
                return
            self._last_object_list_refresh = now
        try:
            objects = self._list_objects()
        except Exception as error:
            logging.warning("cache object listing failed error=%s", error)
            return
        with self._known_objects_lock:
            self._known_objects = objects
        logging.info("cache object listing refreshed count=%d", len(objects))

    def _list_objects(self) -> set[str]:
        list_args = {"Bucket": self._config.bucket}
        prefix = self._object_list_prefix()
        if prefix:
            list_args["Prefix"] = prefix
        paginator = self._s3.get_paginator("list_objects_v2")
        objects = set()
        for page in paginator.paginate(**list_args):
            for item in page.get("Contents", []):
                key = item.get("Key")
                if key is not None:
                    objects.add(key)
        return objects

    def _object_list_prefix(self) -> str:
        if not self._config.prefix:
            return ""
        return f"{self._config.prefix.rstrip('/')}/"

    def _object_key(self, key: bytes) -> str:
        key_hex = key.hex()
        if self._config.layout == "subdirs" and len(key_hex) > 2:
            cache_key = f"{key_hex[:2]}/{key_hex[2:]}"
        else:
            cache_key = key_hex
        if not self._config.prefix:
            return cache_key
        return f"{self._config.prefix.rstrip('/')}/{cache_key}"


class StorageHelperServer(socketserver.ThreadingMixIn, socketserver.UnixStreamServer):
    daemon_threads = True

    def __init__(
        self,
        endpoint: str,
        storage: Storage,
        idle_timeout: float,
        socket_mode: int,
    ) -> None:
        self.storage = storage
        self.idle_timeout = idle_timeout
        self.last_activity = time.monotonic()
        self.stop_event = threading.Event()
        super().__init__(endpoint, StorageHelperHandler)
        os.chmod(endpoint, socket_mode)

    def touch(self) -> None:
        self.last_activity = time.monotonic()

    def serve_until_stopped(self) -> None:
        if self.idle_timeout > 0:
            monitor = threading.Thread(target=self._monitor_idle_timeout, daemon=True)
            monitor.start()
        self.serve_forever(poll_interval=0.5)

    def request_stop(self) -> None:
        self.stop_event.set()
        threading.Thread(target=self.shutdown, daemon=True).start()

    def handle_error(self, request: object, client_address: object) -> None:
        error = sys.exc_info()[1]
        if _is_client_disconnect(error):
            return
        super().handle_error(request, client_address)

    def _monitor_idle_timeout(self) -> None:
        while not self.stop_event.wait(timeout=1):
            if time.monotonic() - self.last_activity >= self.idle_timeout:
                self.request_stop()
                return


class StorageHelperHandler(socketserver.StreamRequestHandler):
    server: StorageHelperServer

    def handle(self) -> None:
        try:
            self.server.touch()
            self.wfile.write(bytes([PROTOCOL_VERSION, 1, CAP_GET_PUT_REMOVE_STOP]))
            self.wfile.flush()
            while True:
                request = self.rfile.read(1)
                if not request:
                    return
                self.server.touch()
                request_code = request[0]
                if request_code == REQUEST_GET:
                    self._handle_get()
                elif request_code == REQUEST_PUT:
                    self._handle_put()
                elif request_code == REQUEST_REMOVE:
                    self._handle_remove()
                elif request_code == REQUEST_STOP:
                    self.wfile.write(bytes([RESPONSE_OK]))
                    self.wfile.flush()
                    self.server.request_stop()
                    return
                else:
                    self._write_error(f"unknown request {request_code}")
                    return
        except (OSError, TimeoutError) as error:
            if _is_client_disconnect(error):
                return
            raise

    def _handle_get(self) -> None:
        key = _read_key(self.rfile)
        try:
            value = self.server.storage.get(key)
        except Exception as error:
            self._write_error(str(error))
            return
        if value is None:
            self.wfile.write(bytes([RESPONSE_NOOP]))
            self.wfile.flush()
            return
        self.wfile.write(bytes([RESPONSE_OK]))
        _write_value(self.wfile, value)
        self.wfile.flush()

    def _handle_put(self) -> None:
        key = _read_key(self.rfile)
        flags = _read_exact(self.rfile, 1)[0]
        value = _read_value(self.rfile)
        try:
            stored = self.server.storage.put(
                key,
                value,
                overwrite=bool(flags & PUT_OVERWRITE),
            )
        except Exception as error:
            self._write_error(str(error))
            return
        self.wfile.write(bytes([RESPONSE_OK if stored else RESPONSE_NOOP]))
        self.wfile.flush()

    def _handle_remove(self) -> None:
        key = _read_key(self.rfile)
        try:
            removed = self.server.storage.remove(key)
        except Exception as error:
            self._write_error(str(error))
            return
        self.wfile.write(bytes([RESPONSE_OK if removed else RESPONSE_NOOP]))
        self.wfile.flush()

    def _write_error(self, message: str) -> None:
        self.wfile.write(bytes([RESPONSE_ERR]))
        _write_message(self.wfile, message)
        self.wfile.flush()


class TerminalLogHandler(logging.Handler):
    def __init__(self, ui: ForegroundUi) -> None:
        super().__init__()
        self._ui = ui

    def emit(self, record: logging.LogRecord) -> None:
        try:
            self._ui.add_log(self.format(record))
        except Exception:
            self.handleError(record)


class ForegroundUi:
    def __init__(
        self,
        storage: S3Storage,
        endpoint: str,
        daemonize_event: threading.Event,
        stop_callback: Callable[[], None],
    ) -> None:
        self._storage = storage
        self._endpoint = endpoint
        self._daemonize_event = daemonize_event
        self._stop_callback = stop_callback
        self._logs: collections.deque[str] = collections.deque(maxlen=10)
        self._logs_lock = threading.Lock()
        self._stop_event = threading.Event()
        self._started_at = time.monotonic()
        self._old_termios: list[int | bytes] | None = None
        self._thread = threading.Thread(target=self._run, daemon=True)
        self.handler = TerminalLogHandler(self)
        self.handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))

    def start(self) -> None:
        self._enter_terminal_mode()
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        self._thread.join(timeout=1)
        self._draw(final=True)
        self._leave_terminal_mode()

    def add_log(self, message: str) -> None:
        with self._logs_lock:
            self._logs.append(message)

    def _run(self) -> None:
        while not self._stop_event.wait(timeout=0.2):
            self._read_input()
            self._draw(final=False)

    def _read_input(self) -> None:
        if not sys.stdin.isatty():
            return
        try:
            readable, _, _ = select.select([sys.stdin], [], [], 0)
        except OSError:
            return
        if not readable:
            return
        try:
            key = os.read(sys.stdin.fileno(), 1)
        except OSError:
            return
        if key.lower() == b"d":
            self._daemonize_event.set()
            self._stop_callback()

    def _draw(self, final: bool) -> None:
        if not sys.stdout.isatty():
            return
        columns = shutil.get_terminal_size(fallback=(100, 24)).columns
        with self._logs_lock:
            logs = list(self._logs)
        visible_logs = [_truncate(line, columns) for line in logs[-10:]]
        lines = [""] * (10 - len(visible_logs)) + visible_logs
        status = self._status_line(columns, final)
        output = ["\x1b[s", "\x1b[?25l", "\x1b[11A"]
        for index in range(10):
            line = lines[index] if index < len(lines) else ""
            output.append("\x1b[2K")
            output.append(line)
            output.append("\n")
        output.append("\x1b[7m")
        output.append(_pad(status, columns))
        output.append("\x1b[0m")
        output.append("\x1b[u")
        if final:
            output.append("\x1b[?25h")
        sys.stdout.write("".join(output))
        sys.stdout.flush()

    def _status_line(self, columns: int, final: bool) -> str:
        elapsed = int(time.monotonic() - self._started_at)
        left = (
            f"ccache S3 helper {'stopping' if final else 'running'} "
            f"{elapsed // 60:02d}:{elapsed % 60:02d} "
            f"{self._storage.status()} socket {self._endpoint}"
        )
        right = "press 'd' to daemonize"
        if len(left) + len(right) + 1 >= columns:
            return _truncate(f"{left} {right}", columns)
        return f"{left}{' ' * (columns - len(left) - len(right))}{right}"

    def _enter_terminal_mode(self) -> None:
        if not sys.stdin.isatty() or not sys.stdout.isatty():
            return
        self._old_termios = termios.tcgetattr(sys.stdin.fileno())
        tty.setcbreak(sys.stdin.fileno())
        sys.stdout.write("\n" * 11)
        sys.stdout.flush()

    def _leave_terminal_mode(self) -> None:
        if self._old_termios is not None:
            termios.tcsetattr(sys.stdin.fileno(), termios.TCSADRAIN, self._old_termios)
            self._old_termios = None
        if sys.stdout.isatty():
            sys.stdout.write("\n")
            sys.stdout.flush()


def _truncate(value: str, columns: int) -> str:
    if columns <= 0:
        return ""
    value = value.replace("\n", " ")
    if len(value) <= columns:
        return value
    if columns == 1:
        return "."
    return f"{value[: columns - 1]}."


def _pad(value: str, columns: int) -> str:
    value = _truncate(value, columns)
    return value + " " * max(0, columns - len(value))


def _read_exact(input_file: BinaryIO, size: int) -> bytes:
    data = input_file.read(size)
    if len(data) != size:
        raise EOFError("unexpected end of request")
    return data


def _read_key(input_file: BinaryIO) -> bytes:
    size = _read_exact(input_file, 1)[0]
    return _read_exact(input_file, size)


def _read_value(input_file: BinaryIO) -> bytes:
    size = int.from_bytes(_read_exact(input_file, 8), sys.byteorder)
    return _read_exact(input_file, size)


def _write_value(output_file: BinaryIO, value: bytes) -> None:
    output_file.write(len(value).to_bytes(8, sys.byteorder))
    output_file.write(value)


def _write_message(output_file: BinaryIO, message: str) -> None:
    data = message.encode("utf-8", errors="replace")[:255]
    output_file.write(bytes([len(data)]))
    output_file.write(data)


def _is_client_disconnect(error: BaseException | None) -> bool:
    if isinstance(error, (BrokenPipeError, ConnectionResetError, TimeoutError)):
        return True
    return isinstance(error, OSError) and error.errno in {
        errno.ECONNRESET,
        errno.EPIPE,
    }


def _client_error_message(error: Exception) -> str:
    response = getattr(error, "response", {})
    error_data = response.get("Error", {})
    code = error_data.get("Code", "S3Error")
    message = error_data.get("Message", str(error))
    return f"{code}: {message}"


def _is_not_found(error: Exception) -> bool:
    response = getattr(error, "response", {})
    status_code = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    code = response.get("Error", {}).get("Code")
    return status_code == 404 or code in {"404", "NoSuchKey", "NotFound"}


def _error_code(error: Exception) -> str:
    response = getattr(error, "response", {})
    return str(response.get("Error", {}).get("Code", ""))


def _is_error_code(error: Exception, *codes: str) -> bool:
    return _error_code(error) in codes


def _attrs_from_env() -> dict[str, str]:
    count_text = os.environ.get("CRSH_NUM_ATTR", "0")
    try:
        count = int(count_text)
    except ValueError:
        return {}
    attrs = {}
    for index in range(count):
        key = os.environ.get(f"CRSH_ATTR_KEY_{index}")
        value = os.environ.get(f"CRSH_ATTR_VALUE_{index}")
        if key is not None and value is not None:
            attrs[key] = value
    return attrs


def _parse_url(url: str, attrs: dict[str, str]) -> S3Config:
    parsed = urlparse(url)
    if parsed.scheme != "s3":
        raise StorageError(f"unsupported storage URL scheme: {parsed.scheme}")
    if not parsed.netloc:
        raise StorageError("S3 storage URL must include a bucket")

    layout = attrs.get("layout", "subdirs")
    if layout not in {"flat", "subdirs"}:
        raise StorageError("layout must be 'flat' or 'subdirs'")

    endpoint_url = (
        attrs.get("endpoint-url")
        or os.environ.get("AWS_ENDPOINT_URL_S3")
        or os.environ.get("AWS_ENDPOINT_URL")
    )
    auth = (
        attrs.get("auth")
        or os.environ.get("CCACHE_S3_AUTH")
        or ("credentials" if endpoint_url else "aws-sso")
    )
    if auth not in {"aws-sso", "aws", "credentials", "cloudflare-r2-oidc-broker"}:
        raise StorageError(
            "auth must be one of 'aws-sso', 'aws', 'credentials', or "
            "'cloudflare-r2-oidc-broker'",
        )
    expected_account_id = (
        attrs.get("aws-account-id")
        or os.environ.get("CCACHE_AWS_ACCOUNT_ID")
        or (DEFAULT_INFRASTRUCTURE_ACCOUNT_ID if auth == "aws-sso" else None)
    )

    return S3Config(
        bucket=parsed.netloc,
        prefix=unquote(parsed.path.lstrip("/")),
        region=(
            attrs.get("region")
            or os.environ.get("CCACHE_S3_REGION")
            or os.environ.get("AWS_REGION")
        ),
        endpoint_url=endpoint_url,
        profile=attrs.get("profile") or os.environ.get("AWS_PROFILE"),
        auth=auth,
        expected_account_id=expected_account_id,
        access_key_id=None,
        secret_access_key=None,
        session_token=None,
        credential_expiration=None,
        oidc_broker_url=(
            attrs.get("oidc-broker-url") or os.environ.get("CCACHE_R2_OIDC_BROKER_URL")
        ),
        oidc_audience=(
            attrs.get("oidc-audience")
            or os.environ.get("CCACHE_R2_OIDC_AUDIENCE")
            or "ccache-r2-broker"
        ),
        layout=layout,
        max_pool_connections=_positive_int(
            attrs.get("max-pool-connections")
            or os.environ.get("CCACHE_S3_MAX_POOL_CONNECTIONS", "64"),
        ),
        object_list_min_interval=_positive_float(
            attrs.get("object-list-min-interval")
            or os.environ.get("CCACHE_S3_OBJECT_LIST_MIN_INTERVAL", "300"),
        ),
        upload_queue_size=_positive_int(
            attrs.get("upload-queue-size")
            or os.environ.get("CCACHE_S3_UPLOAD_QUEUE_SIZE", "4096"),
        ),
        upload_workers=_positive_int(
            attrs.get("upload-workers")
            or os.environ.get("CCACHE_S3_UPLOAD_WORKERS", "8"),
        ),
        upload_drain_timeout=_positive_float(
            attrs.get("upload-drain-timeout")
            or os.environ.get("CCACHE_S3_UPLOAD_DRAIN_TIMEOUT", "60"),
        ),
    )


def _default_endpoint() -> str:
    runtime_dir = os.environ.get("XDG_RUNTIME_DIR") or "/tmp"
    return os.path.join(runtime_dir, "tenzir-ccache", "s3.sock")


def _default_state_dir() -> str:
    return os.path.dirname(_default_endpoint())


def _default_log_file() -> str:
    return os.path.join(_default_state_dir(), "helper.log")


def _default_url() -> str | None:
    if url := os.environ.get("CRSH_URL"):
        return url
    bucket = os.environ.get("CCACHE_S3_BUCKET")
    if not bucket:
        return None
    prefix = os.environ.get("CCACHE_S3_PREFIX", "tenzir").strip("/")
    if not prefix:
        return f"s3://{bucket}"
    return f"s3://{bucket}/{prefix}"


def _is_interactive() -> bool:
    return sys.stdin.isatty() and sys.stderr.isatty()


def _copy_file_if_present(source: str, destination: str) -> None:
    if os.path.isfile(source):
        os.makedirs(os.path.dirname(destination), exist_ok=True)
        shutil.copy2(source, destination)


def _prepare_writable_aws_home() -> None:
    original_home = os.environ.get("HOME")
    if not original_home:
        return
    aws_dir = os.path.join(original_home, ".aws")
    if not os.path.isdir(aws_dir):
        return
    writable_home = os.path.join(_default_state_dir(), "home")
    writable_aws_dir = os.path.join(writable_home, ".aws")
    os.makedirs(os.path.join(writable_aws_dir, "sso", "cache"), exist_ok=True)
    for name in ("config", "credentials"):
        _copy_file_if_present(
            os.path.join(aws_dir, name),
            os.path.join(writable_aws_dir, name),
        )
    source_cache = os.path.join(aws_dir, "sso", "cache")
    destination_cache = os.path.join(writable_aws_dir, "sso", "cache")
    if os.path.isdir(source_cache):
        for name in os.listdir(source_cache):
            _copy_file_if_present(
                os.path.join(source_cache, name),
                os.path.join(destination_cache, name),
            )
    os.environ["HOME"] = writable_home


def _run_aws_sso_login(profile: str, account_id: str) -> None:
    aws = shutil.which("aws")
    if not aws:
        raise StorageError(
            f"AWS login is required for account {account_id}. Install the AWS CLI "
            "and log in to that account.",
        )
    print(
        f"AWS login is required for account {account_id}.",
        file=sys.stderr,
    )
    answer = input("Run AWS SSO login now? [Y/n]: ").strip()
    if answer.lower() in {"n", "no"}:
        raise StorageError(f"AWS login is required for account {account_id}")
    subprocess.run([aws, "sso", "login", "--profile", profile], check=True)


@contextlib.contextmanager
def _quiet_botocore_credential_refresh() -> Iterator[None]:
    loggers = [
        logging.getLogger("botocore.credentials"),
        logging.getLogger("botocore.tokens"),
    ]
    old_levels = [logger.level for logger in loggers]
    try:
        for logger in loggers:
            logger.setLevel(logging.ERROR)
        yield
    finally:
        for logger, old_level in zip(loggers, old_levels, strict=True):
            logger.setLevel(old_level)


def _matching_aws_profiles(account_id: str) -> list[str]:
    try:
        import botocore.session
    except ImportError as error:
        raise StorageError("botocore is required for AWS profile discovery") from error

    session = botocore.session.Session()
    profiles = session.full_config.get("profiles", {})
    matches = [
        name
        for name, values in sorted(profiles.items())
        if values.get("sso_account_id") == account_id
    ]
    return matches


def _select_aws_profile(account_id: str) -> str:
    matches = _matching_aws_profiles(account_id)
    if not matches:
        raise StorageError(
            f"No AWS SSO profile is configured for account {account_id}. "
            "Create one with 'aws configure sso'.",
        )
    if len(matches) == 1 or not _is_interactive():
        return matches[0]

    print(f"Select an AWS profile for account {account_id}:", file=sys.stderr)
    for index, profile in enumerate(matches, start=1):
        print(f"  {index}. {profile}", file=sys.stderr)
    while True:
        answer = input(f"Profile [1-{len(matches)}]: ").strip()
        if not answer:
            return matches[0]
        try:
            selected = int(answer)
        except ValueError:
            print("Enter a number from the list.", file=sys.stderr)
            continue
        if 1 <= selected <= len(matches):
            return matches[selected - 1]
        print("Enter a number from the list.", file=sys.stderr)


def _resolve_aws_profile(config: S3Config) -> S3Config:
    if config.profile or config.auth != "aws-sso":
        return config
    if not config.expected_account_id:
        raise StorageError("aws-sso auth requires an expected AWS account ID")
    profile = _select_aws_profile(config.expected_account_id)
    return S3Config(
        bucket=config.bucket,
        prefix=config.prefix,
        region=config.region,
        endpoint_url=config.endpoint_url,
        profile=profile,
        auth=config.auth,
        expected_account_id=config.expected_account_id,
        access_key_id=config.access_key_id,
        secret_access_key=config.secret_access_key,
        session_token=config.session_token,
        credential_expiration=config.credential_expiration,
        oidc_broker_url=config.oidc_broker_url,
        oidc_audience=config.oidc_audience,
        layout=config.layout,
        max_pool_connections=config.max_pool_connections,
        object_list_min_interval=config.object_list_min_interval,
        upload_queue_size=config.upload_queue_size,
        upload_workers=config.upload_workers,
        upload_drain_timeout=config.upload_drain_timeout,
    )


def _github_actions_oidc_token(audience: str) -> str:
    request_url = os.environ.get("ACTIONS_ID_TOKEN_REQUEST_URL")
    request_token = os.environ.get("ACTIONS_ID_TOKEN_REQUEST_TOKEN")
    if not request_url or not request_token:
        raise StorageError(
            "GitHub Actions OIDC is unavailable; expected "
            "ACTIONS_ID_TOKEN_REQUEST_URL and ACTIONS_ID_TOKEN_REQUEST_TOKEN",
        )
    separator = "&" if "?" in request_url else "?"
    url = f"{request_url}{separator}audience={audience}"
    request = Request(url, headers={"Authorization": f"Bearer {request_token}"})
    with urlopen(request, timeout=30) as response:
        data = json.loads(response.read().decode("utf-8"))
    token = data.get("value")
    if not isinstance(token, str) or not token:
        raise StorageError("GitHub Actions OIDC response did not contain a token")
    return token


def _default_credential_expiration() -> str:
    return (datetime.now(UTC) + timedelta(minutes=50)).isoformat()


def _cloudflare_r2_oidc_broker_response(config: S3Config) -> dict[str, object]:
    if not config.oidc_broker_url:
        raise StorageError(
            "cloudflare-r2-oidc-broker auth requires CCACHE_R2_OIDC_BROKER_URL "
            "or @oidc-broker-url",
        )
    token = _github_actions_oidc_token(config.oidc_audience)
    payload = json.dumps({"token": token}).encode("utf-8")
    request = Request(
        config.oidc_broker_url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urlopen(request, timeout=30) as response:
        data = json.loads(response.read().decode("utf-8"))
    if not isinstance(data, dict):
        raise StorageError("R2 OIDC broker response is not a JSON object")
    return data


def _cloudflare_r2_oidc_broker_metadata(config: S3Config) -> dict[str, str]:
    data = _cloudflare_r2_oidc_broker_response(config)
    return _cloudflare_r2_oidc_broker_metadata_from_response(config, data)


def _cloudflare_r2_oidc_broker_metadata_from_response(
    config: S3Config,
    data: dict[str, object],
) -> dict[str, str]:
    try:
        access_key_id = data["accessKeyId"]
        secret_access_key = data["secretAccessKey"]
        session_token = data["sessionToken"]
        bucket = data["bucket"]
    except KeyError as error:
        raise StorageError(f"R2 OIDC broker response is missing {error.args[0]}") from error
    if not all(
        isinstance(value, str) and value
        for value in (access_key_id, secret_access_key, session_token, bucket)
    ):
        raise StorageError("R2 OIDC broker response contains invalid credentials")
    if bucket != config.bucket:
        raise StorageError(f"R2 OIDC broker returned bucket {bucket}, expected {config.bucket}")
    expires_at = data.get("expiresAt")
    if not isinstance(expires_at, str) or not expires_at:
        expires_at = _default_credential_expiration()
    return {
        "access_key": access_key_id,
        "secret_key": secret_access_key,
        "token": session_token,
        "expiry_time": expires_at,
    }


def _cloudflare_r2_oidc_broker_credentials(config: S3Config) -> S3Config:
    data = _cloudflare_r2_oidc_broker_response(config)
    metadata = _cloudflare_r2_oidc_broker_metadata_from_response(config, data)
    endpoint_url = data.get("endpointUrl")
    region = data.get("region")
    if not isinstance(endpoint_url, str) or not endpoint_url:
        raise StorageError("R2 OIDC broker response is missing endpointUrl")
    if not isinstance(region, str) or not region:
        region = "auto"
    return S3Config(
        bucket=config.bucket,
        prefix=config.prefix,
        region=region,
        endpoint_url=endpoint_url,
        profile=None,
        auth=config.auth,
        expected_account_id=config.expected_account_id,
        access_key_id=metadata["access_key"],
        secret_access_key=metadata["secret_key"],
        session_token=metadata["token"],
        credential_expiration=metadata["expiry_time"],
        oidc_broker_url=config.oidc_broker_url,
        oidc_audience=config.oidc_audience,
        layout=config.layout,
        max_pool_connections=config.max_pool_connections,
        object_list_min_interval=config.object_list_min_interval,
        upload_queue_size=config.upload_queue_size,
        upload_workers=config.upload_workers,
        upload_drain_timeout=config.upload_drain_timeout,
    )


def _initialize_auth(config: S3Config) -> S3Config:
    if config.auth == "cloudflare-r2-oidc-broker":
        return _cloudflare_r2_oidc_broker_credentials(config)
    if config.auth == "credentials" or (
        config.auth == "aws" and not config.expected_account_id
    ):
        return config

    try:
        import boto3
        from botocore.exceptions import BotoCoreError, ClientError
    except ImportError as error:
        raise StorageError("boto3 is required for S3 ccache storage") from error

    resolved_config = _resolve_aws_profile(config)
    session_args = {}
    if resolved_config.profile:
        session_args["profile_name"] = resolved_config.profile
    if resolved_config.region:
        session_args["region_name"] = resolved_config.region

    def caller_account() -> str:
        session = boto3.Session(**session_args)
        with _quiet_botocore_credential_refresh():
            return session.client("sts").get_caller_identity()["Account"]

    try:
        account_id = caller_account()
    except (BotoCoreError, ClientError, OSError) as error:
        if resolved_config.auth == "aws-sso" and resolved_config.profile and _is_interactive():
            _run_aws_sso_login(
                resolved_config.profile,
                resolved_config.expected_account_id or DEFAULT_INFRASTRUCTURE_ACCOUNT_ID,
            )
            account_id = caller_account()
        else:
            if resolved_config.expected_account_id:
                message = f"AWS login is required for account {resolved_config.expected_account_id}"
            else:
                message = "AWS credentials are required"
            raise StorageError(
                message,
            ) from error
    if resolved_config.expected_account_id and account_id != resolved_config.expected_account_id:
        raise StorageError(
            f"AWS credentials are for account {account_id}, "
            f"expected {resolved_config.expected_account_id}",
        )
    return resolved_config


def _daemonize(log_file: str) -> None:
    if not hasattr(os, "fork"):
        raise StorageError("--deamonize is only supported on Unix-like systems")

    os.makedirs(os.path.dirname(log_file) or ".", exist_ok=True)
    pid = os.fork()
    if pid > 0:
        print(f"ccache S3 helper started in background; log: {log_file}", flush=True)
        os._exit(0)

    os.setsid()
    signal.signal(signal.SIGHUP, signal.SIG_IGN)
    pid = os.fork()
    if pid > 0:
        os._exit(0)

    stdin_fd = os.open(os.devnull, os.O_RDONLY)
    log_fd = os.open(log_file, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    try:
        os.dup2(stdin_fd, sys.stdin.fileno())
        os.dup2(log_fd, sys.stdout.fileno())
        os.dup2(log_fd, sys.stderr.fileno())
    finally:
        os.close(stdin_fd)
        os.close(log_fd)


def _remove_stale_socket(endpoint: str) -> None:
    try:
        mode = os.stat(endpoint).st_mode
    except FileNotFoundError:
        return
    if not stat.S_ISSOCK(mode):
        raise StorageError(
            f"IPC endpoint already exists and is not a socket: {endpoint}"
        )
    probe = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        probe.connect(endpoint)
    except OSError as error:
        if error.errno not in {errno.ECONNREFUSED, errno.ENOENT}:
            raise StorageError(
                f"could not probe existing IPC endpoint {endpoint}: {error}",
            ) from error
        os.unlink(endpoint)
    else:
        raise StorageError(f"IPC endpoint is already accepting connections: {endpoint}")
    finally:
        probe.close()


def _parse_mode(mode: str) -> int:
    try:
        return int(mode, 8)
    except ValueError as error:
        raise argparse.ArgumentTypeError(
            "socket mode must be an octal value"
        ) from error


def _positive_float(value: str) -> float:
    try:
        result = float(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("timeout must be numeric") from error
    if result < 0:
        raise argparse.ArgumentTypeError("timeout must be non-negative")
    return result


def _positive_int(value: str) -> int:
    try:
        result = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("value must be an integer") from error
    if result <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return result


def _log_level(value: str) -> int:
    level = getattr(logging, value.upper(), None)
    if not isinstance(level, int):
        raise argparse.ArgumentTypeError(
            "log level must be one of DEBUG, INFO, WARNING, ERROR, or CRITICAL",
        )
    return level


def _env_first(*names: str) -> str | None:
    for name in names:
        value = os.environ.get(name)
        if value:
            return value
    return None


def _role_name_from_arn(role_arn: str | None) -> str | None:
    if not role_arn:
        return None
    match = re.fullmatch(r"arn:aws[a-zA-Z-]*:iam::\d{12}:role/(.+)", role_arn)
    if not match:
        return None
    role_path = match.group(1).rstrip("/")
    return role_path.rsplit("/", maxsplit=1)[-1]


def _prompt_value(name: str, default: str | None, required: bool, yes: bool) -> str:
    if yes:
        if default:
            return default
        if required:
            raise StorageError(f"{name} is required")
        return ""

    if sys.stdin.isatty():
        suffix = f" [{default}]" if default else ""
        while True:
            value = input(f"{name}{suffix}: ").strip()
            if value:
                return value
            if default is not None:
                return default
            if not required:
                return ""
            print(f"{name} is required", file=sys.stderr)

    if default:
        logging.info("using %s=%s", name, default)
        return default
    if required:
        raise StorageError(f"{name} is required in non-interactive mode")
    return ""


def _confirm(prompt: str, yes: bool) -> bool:
    if yes:
        return True
    if not sys.stdin.isatty():
        logging.warning("not changing existing resource without --yes: %s", prompt)
        return False
    answer = input(f"{prompt} [y/N]: ").strip().lower()
    return answer in {"y", "yes"}


def _canonical_json(document: object) -> str:
    return json.dumps(document, sort_keys=True, separators=(",", ":"))


def _documents_match(left: object, right: object) -> bool:
    return _canonical_json(left) == _canonical_json(right)


def _normalize_bucket_region(region: str | None) -> str:
    if not region:
        return "us-east-1"
    if region == "EU":
        return "eu-west-1"
    return region


def _object_resource(bucket: str, prefix: str) -> str:
    clean_prefix = prefix.strip("/")
    if clean_prefix:
        return f"arn:aws:s3:::{bucket}/{clean_prefix}/*"
    return f"arn:aws:s3:::{bucket}/*"


def _list_bucket_statement(bucket: str, prefix: str) -> dict[str, object]:
    statement: dict[str, object] = {
        "Sid": "ListCachePrefix",
        "Effect": "Allow",
        "Action": [
            "s3:GetBucketLocation",
            "s3:ListBucket",
        ],
        "Resource": f"arn:aws:s3:::{bucket}",
    }
    clean_prefix = prefix.strip("/")
    if clean_prefix:
        statement["Condition"] = {
            "StringLike": {
                "s3:prefix": [
                    clean_prefix,
                    f"{clean_prefix}/*",
                ],
            },
        }
    return statement


def _s3_policy_document(bucket: str, prefix: str) -> dict[str, object]:
    return {
        "Version": "2012-10-17",
        "Statement": [
            _list_bucket_statement(bucket, prefix),
            {
                "Sid": "UseCacheObjects",
                "Effect": "Allow",
                "Action": [
                    "s3:DeleteObject",
                    "s3:GetObject",
                    "s3:PutObject",
                ],
                "Resource": _object_resource(bucket, prefix),
            },
        ],
    }


def _trust_policy_document(account_id: str, github_subject: str) -> dict[str, object]:
    return {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                    "Federated": (
                        f"arn:aws:iam::{account_id}:oidc-provider/"
                        f"{GITHUB_OIDC_PROVIDER_HOST}"
                    ),
                },
                "Action": "sts:AssumeRoleWithWebIdentity",
                "Condition": {
                    "StringEquals": {
                        f"{GITHUB_OIDC_PROVIDER_HOST}:aud": GITHUB_OIDC_AUDIENCE,
                    },
                    "StringLike": {
                        f"{GITHUB_OIDC_PROVIDER_HOST}:sub": github_subject,
                    },
                },
            },
        ],
    }


def _bucket_name_part(value: str) -> str:
    return re.sub(r"[^a-z0-9.-]+", "-", value.lower()).strip(".-")


def _default_bucket(account_id: str, region: str, repository: str | None) -> str:
    if repository and "/" in repository:
        owner, name = repository.split("/", maxsplit=1)
        bucket = f"{_bucket_name_part(owner)}-{_bucket_name_part(name)}-ccache"
        if bucket != "-ccache":
            return bucket
    return f"tenzir-ccache-{account_id}-{region}"


def _default_github_subject() -> str | None:
    return _env_first("CCACHE_GITHUB_SUBJECT")


def _default_github_repository() -> str | None:
    return _env_first("CCACHE_GITHUB_REPOSITORY", "GITHUB_REPOSITORY")


def _build_setup_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Create or verify AWS resources for the S3 ccache helper",
    )
    parser.add_argument("--bucket", default=os.environ.get("CCACHE_S3_BUCKET"))
    parser.add_argument("--prefix", default=os.environ.get("CCACHE_S3_PREFIX"))
    parser.add_argument(
        "--region",
        default=_env_first("CCACHE_S3_REGION", "AWS_REGION", "AWS_DEFAULT_REGION"),
    )
    parser.add_argument(
        "--role-name",
        default=_role_name_from_arn(os.environ.get("CCACHE_AWS_ROLE_ARN")),
    )
    parser.add_argument(
        "--policy-name", default=os.environ.get("CCACHE_AWS_POLICY_NAME")
    )
    parser.add_argument("--github-repository", default=_default_github_repository())
    parser.add_argument("--github-subject", default=_default_github_subject())
    parser.add_argument(
        "--yes",
        action="store_true",
        help="accept defaults and update mismatched mutable resources",
    )
    return parser


def _resolve_setup_config(args: argparse.Namespace, account_id: str) -> SetupConfig:
    region_default = args.region or DEFAULT_REGION
    region = _prompt_value("CCACHE_S3_REGION", region_default, True, args.yes)
    bucket_default = args.bucket or _default_bucket(
        account_id,
        region,
        args.github_repository,
    )
    bucket = _prompt_value("CCACHE_S3_BUCKET", bucket_default, True, args.yes)
    prefix = _prompt_value("CCACHE_S3_PREFIX", args.prefix or "tenzir", True, args.yes)
    role_name = _prompt_value(
        "IAM role name",
        args.role_name or "tenzir-ccache-s3",
        True,
        args.yes,
    )
    policy_name = _prompt_value(
        "IAM inline policy name",
        args.policy_name or f"{role_name}-s3",
        True,
        args.yes,
    )
    if args.github_subject:
        github_subject = args.github_subject
    else:
        repository = _prompt_value(
            "GitHub repository",
            args.github_repository,
            True,
            args.yes,
        )
        github_subject = f"repo:{repository}:*"
    github_subject = _prompt_value(
        "GitHub OIDC subject condition",
        github_subject,
        True,
        args.yes,
    )
    return SetupConfig(
        bucket=bucket,
        prefix=prefix.strip("/"),
        region=region,
        role_name=role_name,
        policy_name=policy_name,
        github_subject=github_subject,
        yes=args.yes,
    )


def _ensure_bucket(s3_client: object, config: SetupConfig) -> None:
    client_error = __import__("botocore.exceptions").exceptions.ClientError
    try:
        s3_client.head_bucket(Bucket=config.bucket)
    except client_error as error:
        if _is_error_code(error, "404", "NoSuchBucket", "NotFound"):
            create_args: dict[str, object] = {"Bucket": config.bucket}
            if config.region != "us-east-1":
                create_args["CreateBucketConfiguration"] = {
                    "LocationConstraint": config.region,
                }
            s3_client.create_bucket(**create_args)
            logging.info("created S3 bucket %s in %s", config.bucket, config.region)
        elif _is_error_code(error, "403"):
            raise StorageError(
                f"S3 bucket {config.bucket} already exists but is not accessible",
            ) from error
        else:
            raise
    else:
        location = s3_client.get_bucket_location(Bucket=config.bucket).get(
            "LocationConstraint",
        )
        existing_region = _normalize_bucket_region(location)
        if existing_region == config.region:
            logging.info(
                "S3 bucket %s already exists in %s and matches",
                config.bucket,
                config.region,
            )
        else:
            logging.warning(
                "S3 bucket %s already exists in %s, expected %s",
                config.bucket,
                existing_region,
                config.region,
            )
            raise StorageError("cannot change the region of an existing S3 bucket")

    _ensure_public_access_block(s3_client, config)


def _ensure_public_access_block(s3_client: object, config: SetupConfig) -> None:
    client_error = __import__("botocore.exceptions").exceptions.ClientError
    try:
        response = s3_client.get_public_access_block(Bucket=config.bucket)
        current = response["PublicAccessBlockConfiguration"]
    except client_error as error:
        if not _is_error_code(error, "NoSuchPublicAccessBlockConfiguration", "404"):
            raise
        current = None

    if current is None:
        s3_client.put_public_access_block(
            Bucket=config.bucket,
            PublicAccessBlockConfiguration=PUBLIC_ACCESS_BLOCK_CONFIGURATION,
        )
        logging.info("configured public access block for bucket %s", config.bucket)
        return

    if _documents_match(current, PUBLIC_ACCESS_BLOCK_CONFIGURATION):
        logging.info("bucket %s public access block already matches", config.bucket)
        return

    logging.warning("bucket %s public access block exists but differs", config.bucket)
    if _confirm("Update bucket public access block?", config.yes):
        s3_client.put_public_access_block(
            Bucket=config.bucket,
            PublicAccessBlockConfiguration=PUBLIC_ACCESS_BLOCK_CONFIGURATION,
        )
        logging.info("updated public access block for bucket %s", config.bucket)


def _ensure_oidc_provider(iam_client: object, account_id: str) -> None:
    provider_arn = (
        f"arn:aws:iam::{account_id}:oidc-provider/{GITHUB_OIDC_PROVIDER_HOST}"
    )
    client_error = __import__("botocore.exceptions").exceptions.ClientError
    try:
        provider = iam_client.get_open_id_connect_provider(
            OpenIDConnectProviderArn=provider_arn,
        )
    except client_error as error:
        if not _is_error_code(error, "NoSuchEntity"):
            raise
        iam_client.create_open_id_connect_provider(
            Url=GITHUB_OIDC_PROVIDER_URL,
            ClientIDList=[GITHUB_OIDC_AUDIENCE],
        )
        logging.info("created GitHub Actions OIDC provider %s", provider_arn)
        return

    client_ids = set(provider.get("ClientIDList", []))
    if GITHUB_OIDC_AUDIENCE in client_ids:
        logging.info("GitHub Actions OIDC provider already matches")
        return

    logging.warning(
        "GitHub Actions OIDC provider exists but lacks audience %s",
        GITHUB_OIDC_AUDIENCE,
    )
    iam_client.add_client_id_to_open_id_connect_provider(
        OpenIDConnectProviderArn=provider_arn,
        ClientID=GITHUB_OIDC_AUDIENCE,
    )
    logging.info(
        "added audience %s to GitHub Actions OIDC provider", GITHUB_OIDC_AUDIENCE
    )


def _ensure_role(iam_client: object, account_id: str, config: SetupConfig) -> str:
    client_error = __import__("botocore.exceptions").exceptions.ClientError
    desired_trust_policy = _trust_policy_document(account_id, config.github_subject)
    try:
        response = iam_client.get_role(RoleName=config.role_name)
    except client_error as error:
        if not _is_error_code(error, "NoSuchEntity"):
            raise
        response = iam_client.create_role(
            RoleName=config.role_name,
            AssumeRolePolicyDocument=json.dumps(desired_trust_policy),
            Description="Allows GitHub Actions to use the Tenzir ccache S3 bucket",
        )
        role_arn = response["Role"]["Arn"]
        logging.info("created IAM role %s", role_arn)
        return role_arn

    role = response["Role"]
    role_arn = role["Arn"]
    current_trust_policy = role["AssumeRolePolicyDocument"]
    if _documents_match(current_trust_policy, desired_trust_policy):
        logging.info("IAM role %s trust policy already matches", config.role_name)
    else:
        logging.warning(
            "IAM role %s exists but its trust policy differs",
            config.role_name,
        )
        if _confirm("Update IAM role trust policy?", config.yes):
            iam_client.update_assume_role_policy(
                RoleName=config.role_name,
                PolicyDocument=json.dumps(desired_trust_policy),
            )
            logging.info("updated IAM role %s trust policy", config.role_name)
    return role_arn


def _ensure_role_policy(iam_client: object, config: SetupConfig) -> None:
    client_error = __import__("botocore.exceptions").exceptions.ClientError
    desired_policy = _s3_policy_document(config.bucket, config.prefix)
    try:
        response = iam_client.get_role_policy(
            RoleName=config.role_name,
            PolicyName=config.policy_name,
        )
    except client_error as error:
        if not _is_error_code(error, "NoSuchEntity"):
            raise
        iam_client.put_role_policy(
            RoleName=config.role_name,
            PolicyName=config.policy_name,
            PolicyDocument=json.dumps(desired_policy),
        )
        logging.info(
            "created IAM inline policy %s on role %s",
            config.policy_name,
            config.role_name,
        )
        return

    current_policy = response["PolicyDocument"]
    if _documents_match(current_policy, desired_policy):
        logging.info(
            "IAM inline policy %s on role %s already matches",
            config.policy_name,
            config.role_name,
        )
        return

    logging.warning(
        "IAM inline policy %s on role %s exists but differs",
        config.policy_name,
        config.role_name,
    )
    if _confirm("Update IAM inline policy?", config.yes):
        iam_client.put_role_policy(
            RoleName=config.role_name,
            PolicyName=config.policy_name,
            PolicyDocument=json.dumps(desired_policy),
        )
        logging.info(
            "updated IAM inline policy %s on role %s",
            config.policy_name,
            config.role_name,
        )


def _print_setup_environment(config: SetupConfig, role_arn: str) -> None:
    print()
    print("Use these GitHub Actions variables:")
    print(f"CCACHE_S3_BUCKET={config.bucket}")
    print(f"CCACHE_S3_PREFIX={config.prefix}")
    print(f"CCACHE_S3_REGION={config.region}")
    print(f"CCACHE_AWS_ROLE_ARN={role_arn}")
    print()
    print("For a local shell:")
    print(f"export CCACHE_S3_BUCKET={config.bucket!r}")
    print(f"export CCACHE_S3_PREFIX={config.prefix!r}")
    print(f"export CCACHE_S3_REGION={config.region!r}")
    print(f"export CCACHE_AWS_ROLE_ARN={role_arn!r}")


def _cmd_setup(argv: list[str]) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    parser = _build_setup_parser()
    args = parser.parse_args(argv)

    try:
        import boto3
        from botocore.exceptions import BotoCoreError, ClientError
    except ImportError as error:
        raise StorageError("boto3 is required for S3 ccache setup") from error

    region = args.region or DEFAULT_REGION
    session = boto3.Session(region_name=region)
    try:
        account_id = session.client("sts").get_caller_identity()["Account"]
        config = _resolve_setup_config(args, account_id)
        configured_session = boto3.Session(region_name=config.region)
        _ensure_bucket(configured_session.client("s3"), config)
        iam_client = configured_session.client("iam")
        _ensure_oidc_provider(iam_client, account_id)
        role_arn = _ensure_role(iam_client, account_id, config)
        _ensure_role_policy(iam_client, config)
        _print_setup_environment(config, role_arn)
    except (BotoCoreError, ClientError) as error:
        raise StorageError(str(error)) from error
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Serve the ccache remote storage helper protocol with S3 storage",
        epilog="Run 's3-storage-helper.py setup --help' to create AWS resources.",
    )
    parser.add_argument(
        "--endpoint",
        default=os.environ.get("CRSH_IPC_ENDPOINT") or _default_endpoint(),
        help="Unix socket path to listen on; defaults to CRSH_IPC_ENDPOINT or the local Tenzir ccache socket",
    )
    parser.add_argument(
        "--url",
        default=_default_url(),
        help="S3 URL, for example s3://bucket/prefix; defaults to CRSH_URL or CCACHE_S3_BUCKET/CCACHE_S3_PREFIX",
    )
    parser.add_argument(
        "--idle-timeout",
        type=_positive_float,
        default=_positive_float(os.environ.get("CRSH_IDLE_TIMEOUT", "0")),
        help="exit after this many seconds without client activity; 0 disables",
    )
    parser.add_argument(
        "--socket-mode",
        type=_parse_mode,
        default=0o600,
        help="Unix socket mode, in octal; default: 0600",
    )
    parser.add_argument(
        "--log-level",
        type=_log_level,
        default=_log_level(os.environ.get("CCACHE_S3_LOG_LEVEL", "INFO")),
        help="log level for cache activity; default: INFO",
    )
    parser.add_argument(
        "--deamonize",
        "--daemonize",
        action="store_true",
        dest="daemonize",
        help="move into the background after interactive AWS initialization",
    )
    parser.add_argument(
        "--log-file",
        default=os.environ.get("CCACHE_S3_LOG_FILE") or _default_log_file(),
        help="log file used with --deamonize; defaults to the local Tenzir ccache helper log",
    )
    return parser


def _cmd_serve(argv: list[str]) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    logging.basicConfig(level=args.log_level, format="%(levelname)s: %(message)s")

    if not args.endpoint:
        parser.error(
            "--endpoint or CRSH_IPC_ENDPOINT is required for serving; "
            "run 's3-storage-helper.py setup' to configure AWS resources",
        )
    if not args.url:
        parser.error(
            "--url or CRSH_URL is required for serving; "
            "run 's3-storage-helper.py setup' to configure AWS resources",
        )

    attrs = _attrs_from_env()
    config = _parse_url(args.url, attrs)
    _prepare_writable_aws_home()
    config = _initialize_auth(config)
    if args.daemonize:
        _daemonize(args.log_file)

    daemonize_requested = threading.Event()
    while True:
        requested = _run_server(args, config, daemonize_requested)
        if not requested:
            return 0
        daemonize_requested.clear()
        _daemonize(args.log_file)


def _run_server(
    args: argparse.Namespace,
    config: S3Config,
    daemonize_requested: threading.Event,
) -> bool:
    os.makedirs(os.path.dirname(args.endpoint) or ".", exist_ok=True)
    _remove_stale_socket(args.endpoint)
    storage = S3Storage(config)
    old_umask = os.umask(0o077)
    try:
        server = StorageHelperServer(
            args.endpoint,
            storage,
            args.idle_timeout,
            args.socket_mode,
        )
    finally:
        os.umask(old_umask)

    def stop(_signum: int, _frame: object) -> None:
        server.request_stop()

    ui: ForegroundUi | None = None
    root_logger = logging.getLogger()
    old_handlers: list[logging.Handler] | None = None
    if not args.daemonize and _is_interactive():
        ui = ForegroundUi(
            storage, args.endpoint, daemonize_requested, server.request_stop
        )
        old_handlers = root_logger.handlers[:]
        root_logger.handlers = [ui.handler]
        ui.start()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    try:
        server.serve_until_stopped()
    except KeyboardInterrupt:
        server.request_stop()
    finally:
        if ui is not None:
            ui.stop()
            root_logger.handlers = old_handlers or []
        server.server_close()
        storage.close()
    return daemonize_requested.is_set()


def main() -> NoReturn:
    try:
        if len(sys.argv) > 1 and sys.argv[1] == "setup":
            result = _cmd_setup(sys.argv[2:])
        elif len(sys.argv) > 1 and sys.argv[1] == "serve":
            result = _cmd_serve(sys.argv[2:])
        else:
            result = _cmd_serve(sys.argv[1:])
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception as error:
        print(f"s3-storage-helper.py: {error}", file=sys.stderr)
        sys.exit(1)
    sys.exit(result)


if __name__ == "__main__":
    main()
