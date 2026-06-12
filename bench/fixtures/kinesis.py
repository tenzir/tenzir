"""Mock Amazon Kinesis fixture for repo-local benchmarks.

Builds and runs the Go replay server in ``kinesis-mock/`` and points the
operators at it via an endpoint override. The server replays the staged
benchmark dataset through the Kinesis JSON protocol with configurable shard
count, artificial latency, and probabilistic throttling, so benchmarks measure
Tenzir's operator code rather than AWS service quotas.
"""

from __future__ import annotations

import hashlib
import http.client
import logging
import os
import shutil
import subprocess
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path

from tenzir_bench.fixtures import (
    FixtureHandle,
    FixtureUnavailable,
    current_context,
    current_options,
    fixture,
)

_LOG = logging.getLogger(__name__)


@dataclass(frozen=True)
class KinesisFixtureOptions:
    """Structured configuration for the ``kinesis`` benchmark fixture."""

    stream: str = "tenzir-bench"
    shards: int = 1
    latency_ms: int = 0
    throttle_rate: float = 0.0
    max_records: int = 10000
    input_file: str | None = None
    wait_timeout_seconds: float = 60.0


def _mock_source() -> Path:
    return Path(__file__).with_name("kinesis-mock") / "main.go"


def _cache_dir() -> Path:
    root = os.environ.get("XDG_CACHE_HOME")
    base = Path(root) if root else Path.home() / ".cache"
    return base / "tenzir-bench"


def _build_mock() -> Path:
    source = _mock_source()
    if not source.exists():
        raise RuntimeError(f"kinesis mock source does not exist: {source}")
    digest = hashlib.sha1(source.read_bytes()).hexdigest()[:12]
    binary = _cache_dir() / f"kinesis-mock-{digest}"
    if binary.exists():
        return binary
    binary.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        dir=binary.parent, prefix=binary.name, delete=False
    ) as handle:
        staging = Path(handle.name)
    try:
        _LOG.info("Building kinesis mock server from %s", source)
        result = subprocess.run(
            ["go", "build", "-o", str(staging), str(source)],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode != 0:
            detail = (result.stderr or result.stdout or "").strip() or "no output"
            raise RuntimeError(f"go build failed: {detail}")
        staging.chmod(0o755)
        os.replace(staging, binary)
    finally:
        staging.unlink(missing_ok=True)
    return binary


def _resolve_path(benchmark_path: Path, raw: str) -> Path:
    value = raw.strip()
    if not value:
        raise ValueError("'kinesis.input_file' must be a non-empty string")
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = (benchmark_path.parent / path).resolve()
    else:
        path = path.resolve()
    return path


def _count_lines(path: Path) -> int:
    count = 0
    with path.open("rb") as handle:
        for _ in handle:
            count += 1
    return count


def _read_listen_address(process: subprocess.Popen[bytes], timeout: float) -> str:
    deadline = time.monotonic() + timeout
    assert process.stdout is not None
    while time.monotonic() < deadline:
        line = process.stdout.readline().decode("utf-8", errors="replace").strip()
        if line.startswith("LISTENING "):
            return line.removeprefix("LISTENING ")
        if not line and process.poll() is not None:
            break
    raise RuntimeError(
        "kinesis mock server did not report a listen address "
        f"(exit code {process.poll()})"
    )


def _wait_for_health(address: str, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    last_error = "no response"
    while time.monotonic() < deadline:
        try:
            connection = http.client.HTTPConnection(address, timeout=1)
            connection.request("GET", "/healthz")
            if connection.getresponse().status == 200:
                return
        except OSError as exc:
            last_error = str(exc)
        time.sleep(0.05)
    raise RuntimeError(f"kinesis mock server is not healthy: {last_error}")


def _teardown(process: subprocess.Popen[bytes]) -> None:
    process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()


@fixture(name="kinesis", replace=True, options=KinesisFixtureOptions)
def kinesis() -> FixtureHandle:
    """Start a mock Kinesis server seeded with the benchmark dataset."""

    context = current_context()
    if context is None:
        raise RuntimeError("kinesis fixture requires an active benchmark context")
    options = current_options("kinesis")
    if not isinstance(options, KinesisFixtureOptions):
        raise ValueError("invalid options for fixture 'kinesis'")
    if shutil.which("go") is None:
        raise FixtureUnavailable("go toolchain required but not found")
    if options.shards <= 0:
        raise ValueError("'kinesis.shards' must be a positive integer")
    if not 0.0 <= options.throttle_rate <= 1.0:
        raise ValueError("'kinesis.throttle_rate' must be in [0, 1]")
    input_path = context.dataset_path
    if options.input_file is not None:
        input_path = _resolve_path(context.definition.path, options.input_file)
        if not input_path.exists():
            raise RuntimeError(f"kinesis input file does not exist: {input_path}")
    binary = _build_mock()
    process = subprocess.Popen(
        [
            str(binary),
            "--port=0",
            f"--shards={options.shards}",
            f"--latency={options.latency_ms}ms",
            f"--throttle-rate={options.throttle_rate}",
            f"--max-records={options.max_records}",
            f"--dataset={input_path}",
            f"--stream={options.stream}",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
    )
    try:
        address = _read_listen_address(process, options.wait_timeout_seconds)
        _wait_for_health(address, options.wait_timeout_seconds)
    except Exception:
        _teardown(process)
        raise
    count = _count_lines(input_path)
    _LOG.info(
        "Kinesis mock server on %s replays %s records across %s shards "
        "(latency %sms, throttle rate %s)",
        address,
        f"{count:,}",
        options.shards,
        options.latency_ms,
        options.throttle_rate,
    )
    return FixtureHandle(
        env={
            "BENCHMARK_KINESIS_ENDPOINT": f"http://{address}",
            "BENCHMARK_KINESIS_STREAM": options.stream,
            "BENCHMARK_KINESIS_COUNT": str(count),
        },
        teardown=lambda: _teardown(process),
    )
