"""Fixture that manages a transient ``tenzir-node`` instance.

Tests declare ``fixtures: [node]`` to run their pipelines against a fresh node
with its own state and cache directories. The fixture started out as a
built-in of ``tenzir-test``, which drops it from 3.0 on because Tenzir v7 has
no user-facing nodes. This copy relies only on the public fixture API, so it
works with ``tenzir-test`` 2.x, where it replaces the built-in of the same
name, and with 3.x, which has none.

The fixture provides:

- **TENZIR_ENDPOINT** - The node's endpoint, which ``tenzir`` reads as
  ``tenzir.endpoint``. ``tenzir-test`` 2.x also passes it as ``--endpoint``.
- **TENZIR_NODE_CLIENT_ENDPOINT**, **TENZIR_NODE_CLIENT_BINARY**,
  **TENZIR_NODE_CLIENT_TIMEOUT** - The endpoint, client command, and timeout,
  as the ``Executor`` of ``tenzir-test`` 2.x reads them.
- **TENZIR_PYTHON_FIXTURE_ENDPOINT**, **TENZIR_PYTHON_FIXTURE_BINARY**,
  **TENZIR_PYTHON_FIXTURE_TIMEOUT** - The same values, as the ``Executor`` of
  ``tenzir-test`` 3.x reads them.
- **TENZIR_NODE_STATE_DIRECTORY**, **TENZIR_NODE_CACHE_DIRECTORY** - The
  node's directories.
- **TENZIR_NODE_STDOUT_LOG**, **TENZIR_NODE_STDERR_LOG** - The node's output.

The node binary comes from ``TENZIR_NODE_BINARY``, else ``tenzir-node`` on
``PATH``, else ``uvx --from tenzir tenzir-node``. A ``tenzir-node.yaml`` next
to the test configures the node.
"""

from __future__ import annotations

import atexit
import functools
import logging
import os
import shlex
import shutil
import subprocess
import tempfile
import threading
from collections import deque
from contextlib import contextmanager
from pathlib import Path
from dataclasses import dataclass
from typing import IO, Iterator, TYPE_CHECKING
import weakref

from tenzir_test.fixtures import (
    current_context,
    register,
    register_tmp_dir_cleanup,
    unregister_tmp_dir_cleanup,
)

if TYPE_CHECKING:
    from tenzir_test.fixtures import FixtureContext

_STDERR_READ_TIMEOUT = 0.5  # seconds to wait for stderr data
_TAIL_MAX_LINES = 100  # lines of node output kept in memory for failure reports
_PUMP_JOIN_TIMEOUT = 5.0  # seconds to wait for output pumps during teardown


class _OutputPump:
    """Drains a process output stream into a log file on a background thread.

    An undrained pipe blocks the node once the kernel buffer fills up, and its
    contents are lost when the process goes away. The pump prevents both: it
    persists everything to ``log_path`` and keeps a bounded tail in memory for
    failure reports.
    """

    def __init__(self, stream: IO[str], log_path: Path, name: str) -> None:
        self._stream = stream
        self._log_path = log_path
        self._tail: deque[str] = deque(maxlen=_TAIL_MAX_LINES)
        self._lock = threading.Lock()
        self._output_available = threading.Event()
        self._log_path.touch(exist_ok=True)
        self._thread = threading.Thread(target=self._run, name=name, daemon=True)
        self._thread.start()

    def _run(self) -> None:
        try:
            with self._log_path.open("a", encoding="utf-8") as log:
                for line in self._stream:
                    log.write(line)
                    log.flush()
                    with self._lock:
                        self._tail.append(line)
                    self._output_available.set()
        except (OSError, ValueError):  # stream closed during teardown
            pass
        finally:
            # The pump owns the stream: closing it from another thread would
            # block on the reader lock while a read is in flight.
            try:
                self._stream.close()
            except OSError:  # pragma: no cover - defensive cleanup
                pass

    def tail(self) -> str:
        """The most recent output lines, for embedding in error messages."""
        with self._lock:
            return "".join(self._tail).strip()

    def join(self, timeout: float) -> None:
        self._thread.join(timeout)

    def wait_for_output(self, timeout: float) -> None:
        """Wait until the pump captures its first output line or times out."""
        self._output_available.wait(timeout)


def _watch_node_exit(
    process: subprocess.Popen[str],
    stopping: threading.Event,
    stderr_pump: _OutputPump | None,
    lifecycle_lock: threading.Lock,
) -> None:
    """Report a node that exits while the fixture is still active."""
    returncode = process.wait()
    with lifecycle_lock:
        if stopping.is_set():
            return
    tail = ""
    if stderr_pump is not None:
        # The pump reaches EOF once the process is gone; wait for it so the
        # tail includes the node's final output.
        stderr_pump.join(_PUMP_JOIN_TIMEOUT)
        tail = stderr_pump.tail()
    detail = f"; stderr tail:\n{tail}" if tail else ""
    _LOGGER.error("tenzir-node exited unexpectedly with code %s%s", returncode, detail)


def _terminate_process(process: subprocess.Popen[str]) -> None:
    """Terminate the spawned node process and ensure its group is gone."""

    try:
        pgid = os.getpgid(process.pid)
    except OSError:
        pgid = None

    descendants_running = False
    try:
        process.terminate()
        process.wait(timeout=20)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()
    finally:
        if pgid is not None:
            try:
                os.killpg(pgid, 0)
            except ProcessLookupError:
                pass
            else:
                descendants_running = True
    # Leave a helpful error so call sites can report the leak.
    if descendants_running:
        raise RuntimeError("tenzir-node left descendant processes running")


@dataclass(slots=True)
class _TempDirRecord:
    temp_dir: tempfile.TemporaryDirectory[str] | None
    ref: weakref.ref["FixtureContext"]
    path: Path
    process: subprocess.Popen[str] | None = None


_TEMP_DIRS: dict[int, _TempDirRecord] = {}
_TEMP_DIR_LOCK = threading.Lock()
_LOGGER = logging.getLogger(__name__)


def _stop_process(record: _TempDirRecord) -> None:
    process = record.process
    if process is None:
        return
    record.process = None
    try:
        _terminate_process(process)
    except Exception as exc:  # pragma: no cover - defensive logging
        _LOGGER.warning("failed to terminate tenzir-node process: %s", exc)


def _cleanup_record(record: _TempDirRecord) -> None:
    _stop_process(record)
    if record.temp_dir is None:
        return
    try:
        record.temp_dir.cleanup()
    except Exception as exc:  # pragma: no cover - defensive logging
        _LOGGER.debug("failed to remove temporary directory %s: %s", record.path, exc)


def _cleanup_record_by_key(key: int) -> None:
    with _TEMP_DIR_LOCK:
        record = _TEMP_DIRS.pop(key, None)
    if record is None:
        return
    unregister_tmp_dir_cleanup(record.path)
    _cleanup_record(record)


def _ensure_temp_dir(context: "FixtureContext") -> Path:
    key = id(context)
    stale_record: _TempDirRecord | None = None
    with _TEMP_DIR_LOCK:
        record = _TEMP_DIRS.get(key)
        if record is not None:
            current = record.ref()
            if current is context:
                return record.path
            _TEMP_DIRS.pop(key, None)
            stale_record = record
    if stale_record is not None:
        unregister_tmp_dir_cleanup(stale_record.path)
        _cleanup_record(stale_record)

    tmp_root = context.env.get("TENZIR_TMP_DIR") or None
    temp_dir: tempfile.TemporaryDirectory[str] | None
    if tmp_root is None:
        temp_dir = tempfile.TemporaryDirectory(prefix="tenzir-node-")
        path = Path(temp_dir.name)
    else:
        # The harness owns TENZIR_TMP_DIR and removes it after fixture teardown.
        # Leave nested artifacts in place so --keep can preserve them.
        temp_dir = None
        path = Path(tempfile.mkdtemp(prefix="tenzir-node-", dir=tmp_root))

    def _cleanup(dead_ref: weakref.ref["FixtureContext"]) -> None:
        with _TEMP_DIR_LOCK:
            existing = _TEMP_DIRS.get(key)
        if existing is not None and existing.ref is dead_ref:
            _cleanup_record_by_key(key)

    ctx_ref = weakref.ref(context, _cleanup)
    record = _TempDirRecord(temp_dir=temp_dir, ref=ctx_ref, path=path)
    with _TEMP_DIR_LOCK:
        _TEMP_DIRS[key] = record
    register_tmp_dir_cleanup(path, functools.partial(_cleanup_record_by_key, key))
    return path


def _resolve_node_binary(context: "FixtureContext") -> tuple[str, ...] | None:
    """Find the command that starts a node, as ``tenzir-test`` 2.x did.

    ``tenzir-test`` 3.x neither resolves the node binary nor provides
    ``FixtureContext.tenzir_node_binary``, so the fixture looks it up itself.
    """
    resolved = getattr(context, "tenzir_node_binary", None)
    if resolved:
        return tuple(resolved)
    configured = context.env.get("TENZIR_NODE_BINARY") or os.environ.get(
        "TENZIR_NODE_BINARY"
    )
    if configured:
        try:
            parts = tuple(shlex.split(configured))
        except ValueError as exc:
            raise RuntimeError(
                f"invalid shell syntax in TENZIR_NODE_BINARY: {exc}"
            ) from exc
        if parts:
            return parts
    search_path = context.env.get("PATH") or os.environ.get("PATH")
    which_result = shutil.which("tenzir-node", path=search_path)
    if which_result:
        return (which_result,)
    if shutil.which("uvx", path=search_path):
        return ("uvx", "--from", "tenzir", "tenzir-node")
    return None


def _resolve_node_config(context: "FixtureContext") -> str | None:
    """Return the node configuration file, as ``tenzir-test`` 2.x set it.

    A ``tenzir-node.yaml`` next to the test takes precedence; ``tenzir-test``
    2.x exported it as ``TENZIR_NODE_CONFIG``, which 3.x no longer does. For a
    suite, ``context.test`` is its first test.
    """
    node_config_file = context.test.parent / "tenzir-node.yaml"
    if node_config_file.is_file():
        return str(node_config_file)
    return context.env.get("TENZIR_NODE_CONFIG") or None


@contextmanager
def node() -> Iterator[dict[str, str]]:
    """Start ``tenzir-node`` and yield environment data for dependent tests."""

    context = current_context()
    if context is None:
        raise RuntimeError("node fixture requires an active test context")

    node_binary = _resolve_node_binary(context)
    if not node_binary:
        raise RuntimeError(
            "tenzir-node binary not available. The node fixture checks: "
            "TENZIR_NODE_BINARY env var, PATH lookup, uvx fallback. "
            "Ensure tenzir-node is installed, uv is available, or set TENZIR_NODE_BINARY."
        )

    env = context.env.copy()
    # Extract and filter config arguments: we handle --config and --package-dirs separately.
    config_args: list[str] = []
    config_package_dirs: list[str] = []
    for arg in context.config_args:
        if arg.startswith("--config="):
            continue
        if arg.startswith("--package-dirs="):
            value = arg.split("=", 1)[1]
            config_package_dirs.extend(
                entry.strip() for entry in value.split(",") if entry.strip()
            )
            continue
        config_args.append(arg)
    node_config = _resolve_node_config(context)
    if node_config:
        config_args.append(f"--config={node_config}")
    package_args: list[str] = []
    package_root = env.get("TENZIR_PACKAGE_ROOT")
    package_dirs: list[str] = []
    if package_root:
        package_dirs.append(package_root)

    extra_package_dirs = env.get("TENZIR_PACKAGE_DIRS")
    if extra_package_dirs:
        package_dirs.extend(
            [entry.strip() for entry in extra_package_dirs.split(",") if entry.strip()]
        )

    # Include package directories from config_args as well.
    package_dirs.extend(config_package_dirs)

    # Deduplicate while preserving order by using resolved paths as keys.
    seen: set[str] = set()
    unique_dirs: list[str] = []
    for entry in package_dirs:
        resolved = str(Path(entry).expanduser().resolve(strict=False))
        if resolved not in seen:
            seen.add(resolved)
            unique_dirs.append(entry)
    package_dirs = unique_dirs

    if package_dirs:
        package_args.append(f"--package-dirs={','.join(package_dirs)}")
    temp_dir = _ensure_temp_dir(context)
    key = id(context)
    stdout_log = temp_dir / "node-stdout.log"
    stderr_log = temp_dir / "node-stderr.log"

    configured_state = env.get("TENZIR_NODE_STATE_DIRECTORY")
    state_dir = Path(configured_state) if configured_state else temp_dir / "state"
    configured_cache = env.get("TENZIR_NODE_CACHE_DIRECTORY")
    cache_dir = Path(configured_cache) if configured_cache else temp_dir / "cache"
    state_dir.mkdir(parents=True, exist_ok=True)
    cache_dir.mkdir(parents=True, exist_ok=True)
    env.setdefault("TENZIR_NODE_STATE_DIRECTORY", str(state_dir))
    env.setdefault("TENZIR_NODE_CACHE_DIRECTORY", str(cache_dir))

    if context.coverage:
        coverage_dir = env.get(
            "CMAKE_COVERAGE_OUTPUT_DIRECTORY",
            os.path.join(os.getcwd(), "coverage"),
        )
        source_dir = env.get("COVERAGE_SOURCE_DIR", os.getcwd())
        os.makedirs(coverage_dir, exist_ok=True)
        profile_path = os.path.join(
            coverage_dir,
            f"{context.test.stem}-node-%p.profraw",
        )
        env["LLVM_PROFILE_FILE"] = profile_path
        env["COVERAGE_SOURCE_DIR"] = source_dir

    test_root = context.test.parent

    node_cmd = [
        *node_binary,
        "--bare-mode",
        "--console-verbosity=warning",
        f"--state-directory={state_dir}",
        f"--cache-directory={cache_dir}",
        "--endpoint=localhost:0",
        "--print-endpoint",
        *config_args,
        *package_args,
    ]

    if _LOGGER.isEnabledFor(logging.DEBUG):
        command_line = shlex.join(node_cmd)
        _LOGGER.debug("spawning tenzir-node: %s (cwd=%s)", command_line, test_root)

    process = subprocess.Popen(
        node_cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
        env=env,
        start_new_session=True,
        cwd=test_root,
    )
    with _TEMP_DIR_LOCK:
        record = _TEMP_DIRS.get(key)
        if record is not None:
            record.process = process

    stopping = threading.Event()
    lifecycle_lock = threading.Lock()
    stdout_pump: _OutputPump | None = None
    stderr_pump: _OutputPump | None = None
    watcher: threading.Thread | None = None
    try:
        # Start draining stderr before waiting for the endpoint. Otherwise,
        # startup diagnostics can fill the pipe and prevent the node from ever
        # reaching the endpoint print.
        if process.stderr:
            stderr_pump = _OutputPump(process.stderr, stderr_log, "node-stderr-pump")

        endpoint_line = ""
        if process.stdout:
            endpoint_line = process.stdout.readline()
        endpoint = endpoint_line.strip()

        if not endpoint:
            # Collect diagnostic information to help debug startup failures.
            diagnostics: list[str] = []
            returncode = process.poll()
            if returncode is not None:
                diagnostics.append(f"exit code {returncode}")
                if stderr_pump is not None:
                    stderr_pump.join(_PUMP_JOIN_TIMEOUT)
            elif stderr_pump is not None:
                # Give diagnostics already in flight a short opportunity to
                # reach the in-memory tail without blocking on a live stream.
                stderr_pump.wait_for_output(_STDERR_READ_TIMEOUT)
            stderr_output = stderr_pump.tail() if stderr_pump is not None else ""
            if stderr_output:
                diagnostics.append(f"stderr:\n{stderr_output}")
            detail = (
                "; ".join(diagnostics)
                if diagnostics
                else "no additional diagnostics available"
            )
            raise RuntimeError(f"failed to obtain endpoint from tenzir-node ({detail})")

        # Preserve the endpoint line before handing the rest of stdout to the
        # pump, so the log contains the complete stream.
        with stdout_log.open("a", encoding="utf-8") as log:
            log.write(endpoint_line)
        if process.stdout:
            stdout_pump = _OutputPump(process.stdout, stdout_log, "node-stdout-pump")

        # The watcher reports the stderr tail because that is where the node
        # writes its diagnostics; stdout only carries the endpoint line.
        watcher = threading.Thread(
            target=_watch_node_exit,
            args=(process, stopping, stderr_pump, lifecycle_lock),
            name="node-exit-watcher",
            daemon=True,
        )
        watcher.start()

        client_binary: str | None = None
        if context.tenzir_binary:
            client_binary = shlex.join(context.tenzir_binary)
        else:
            client_binary = env.get("TENZIR_BINARY")
        timeout = str(context.config["timeout"])
        fixture_env = {
            # `tenzir-test` 3.x passes no `--endpoint` for this fixture, so
            # clients pick the endpoint up from the environment.
            "TENZIR_ENDPOINT": endpoint,
            # The `Executor` of `tenzir-test` 2.x and many tests read these.
            "TENZIR_NODE_CLIENT_ENDPOINT": endpoint,
            "TENZIR_NODE_CLIENT_BINARY": client_binary,
            "TENZIR_NODE_CLIENT_TIMEOUT": timeout,
            # The `Executor` of `tenzir-test` 3.x reads these.
            "TENZIR_PYTHON_FIXTURE_ENDPOINT": endpoint,
            "TENZIR_PYTHON_FIXTURE_BINARY": client_binary,
            "TENZIR_PYTHON_FIXTURE_TIMEOUT": timeout,
            "TENZIR_NODE_STATE_DIRECTORY": str(state_dir),
            "TENZIR_NODE_CACHE_DIRECTORY": str(cache_dir),
            "TENZIR_NODE_STDOUT_LOG": str(stdout_log),
            "TENZIR_NODE_STDERR_LOG": str(stderr_log),
        }
        # Filter out empty values to avoid polluting the environment.
        filtered_env = {k: v for k, v in fixture_env.items() if v}
        yield filtered_env
    finally:
        # Only classify a running process as an intentional shutdown. The lock
        # lets the watcher report a process that already exited before teardown
        # began, even if its thread has not run yet.
        with lifecycle_lock:
            if process.poll() is None:
                stopping.set()
        with _TEMP_DIR_LOCK:
            record = _TEMP_DIRS.get(key)
        if record is not None:
            _stop_process(record)
        else:  # pragma: no cover - defensive logging
            _LOGGER.debug("node fixture context missing record for key %s", key)
            try:
                _terminate_process(process)
            except Exception as exc:
                _LOGGER.warning(
                    "failed to terminate leaked tenzir-node (key=%s): %s", key, exc
                )
        # The pumps close their streams themselves once they hit EOF. Close
        # only streams that were never handed to a pump.
        if stdout_pump is not None:
            stdout_pump.join(_PUMP_JOIN_TIMEOUT)
        elif process.stdout:
            process.stdout.close()
        if stderr_pump is not None:
            stderr_pump.join(_PUMP_JOIN_TIMEOUT)
        elif process.stderr:
            process.stderr.close()
        if watcher is not None:
            watcher.join(_PUMP_JOIN_TIMEOUT)


# The harness imports its built-in fixtures before project fixtures, so this
# replaces the built-in `node` of `tenzir-test` 2.x. From 3.x on, there is no
# built-in left to replace.
register("node", node, replace=True)


@atexit.register
def _cleanup_all_node_temp_dirs() -> None:  # pragma: no cover - interpreter shutdown
    keys: list[int]
    with _TEMP_DIR_LOCK:
        keys = list(_TEMP_DIRS.keys())
    for key in keys:
        _cleanup_record_by_key(key)


__all__ = ["node"]
