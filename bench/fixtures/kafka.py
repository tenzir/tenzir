"""Shared fixtures for repo-local benchmarks."""

from __future__ import annotations

import hashlib
import logging
import re
import shutil
import subprocess
import time
import uuid
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
_FIXTURE_BROKERS = "127.0.0.1:9092"


@dataclass(frozen=True)
class KafkaFixtureOptions:
    """Structured configuration for the ``kafka`` benchmark fixture."""

    service: str = "redpanda"
    topic: str = "tenzir-bench"
    input_file: str | None = None
    repetitions: int = 1
    bootstrap_servers: str = _FIXTURE_BROKERS
    partitions: int = 1
    replication_factor: int = 1
    wait_timeout_seconds: float = 120.0
    wait_poll_interval_seconds: float = 1.0


def _compose_available() -> bool:
    if shutil.which("docker") is None:
        return False
    try:
        result = subprocess.run(
            ["docker", "compose", "version"],
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError:
        return False
    return result.returncode == 0


def _resolve_path(benchmark_path: Path, raw: str) -> Path:
    value = raw.strip()
    if not value:
        raise ValueError("'kafka.compose_file' must be a non-empty string")
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = (benchmark_path.parent / path).resolve()
    else:
        path = path.resolve()
    return path


def _default_compose_file() -> Path:
    return Path(__file__).with_name("compose.yaml").resolve()


def _project_name(benchmark_path: Path) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", benchmark_path.stem.lower()).strip("-") or "kafka"
    digest = hashlib.sha1(str(benchmark_path).encode("utf-8")).hexdigest()[:8]
    return f"tenzir-bench-{slug[:24]}-{digest}"


def _group_id(benchmark_path: Path, topic: str) -> str:
    digest = hashlib.sha1(f"{benchmark_path}\0{topic}".encode("utf-8")).hexdigest()[:12]
    return f"tenzir-bench-{digest}"


def _compose_base_args(*, compose_file: Path, project_name: str) -> list[str]:
    return ["docker", "compose", "-f", str(compose_file), "-p", project_name]


def _run(
    cmd: list[str],
    *,
    cwd: Path,
    description: str,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        cmd,
        cwd=cwd,
        capture_output=True,
        text=True,
        check=False,
    )
    if not check or result.returncode == 0:
        return result
    detail = (result.stderr or result.stdout or "").strip() or "no output"
    raise RuntimeError(f"{description} failed (exit code {result.returncode}): {detail}")


def _wait_for_cluster(
    base_args: list[str],
    *,
    cwd: Path,
    service: str,
    timeout_seconds: float,
    poll_interval_seconds: float,
) -> None:
    deadline = time.monotonic() + timeout_seconds
    last_detail = "no output"
    while time.monotonic() < deadline:
        result = _run(
            [
                *base_args,
                "exec",
                "-T",
                service,
                "rpk",
                "topic",
                "list",
                "--brokers",
                _FIXTURE_BROKERS,
            ],
            cwd=cwd,
            description="kafka readiness probe",
            check=False,
        )
        if result.returncode == 0:
            return
        last_detail = (result.stderr or result.stdout or "").strip() or "no output"
        time.sleep(poll_interval_seconds)
    raise RuntimeError(
        "kafka fixture did not become ready within "
        f"{timeout_seconds:.0f}s: {last_detail}",
    )


def _reset_topic(
    base_args: list[str],
    *,
    cwd: Path,
    service: str,
    topic: str,
    partitions: int,
    replication_factor: int,
) -> None:
    _run(
        [
            *base_args,
            "exec",
            "-T",
            service,
            "rpk",
            "topic",
            "delete",
            topic,
            "--brokers",
            _FIXTURE_BROKERS,
        ],
        cwd=cwd,
        description=f"delete kafka topic {topic}",
        check=False,
    )
    _run(
        [
            *base_args,
            "exec",
            "-T",
            service,
            "rpk",
            "topic",
            "create",
            topic,
            "--partitions",
            str(partitions),
            "--replicas",
            str(replication_factor),
            "--brokers",
            _FIXTURE_BROKERS,
        ],
        cwd=cwd,
        description=f"create kafka topic {topic}",
    )


def _publish_dataset(
    base_args: list[str],
    *,
    cwd: Path,
    service: str,
    topic: str,
    input_path: Path,
    repetitions: int,
) -> None:
    source_size = input_path.stat().st_size
    source_records = sum(1 for _ in input_path.open("rb"))
    total_records = source_records * repetitions
    total_bytes = source_size * repetitions
    _LOG.info(
        "Seeding Kafka topic %s with %s records (%.1f MiB) from %s repeated %sx",
        topic,
        f"{total_records:,}",
        total_bytes / (1024 * 1024),
        input_path,
        repetitions,
    )
    started_at = time.monotonic()
    process = subprocess.Popen(
        [
            *base_args,
            "exec",
            "-T",
            service,
            "rpk",
            "topic",
            "produce",
            topic,
            "--brokers",
            _FIXTURE_BROKERS,
        ],
        cwd=cwd,
        stdin=subprocess.PIPE,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )
    try:
        if process.stdin is None:
            raise RuntimeError("failed to open stdin for kafka dataset publisher")
        with input_path.open("rb") as handle:
            for _ in range(repetitions):
                handle.seek(0)
                shutil.copyfileobj(handle, process.stdin)
        process.stdin.close()
        stdout, stderr = process.communicate()
    except Exception:
        process.kill()
        process.wait()
        raise
    if process.returncode == 0:
        _LOG.info(
            "Finished seeding Kafka topic %s in %.1fs",
            topic,
            time.monotonic() - started_at,
        )
        return
    detail = (stderr or stdout or b"").decode("utf-8", errors="replace").strip() or "no output"
    raise RuntimeError(
        "publish benchmark dataset to kafka topic "
        f"{topic} failed (exit code {process.returncode}): {detail}",
    )


def _teardown(base_args: list[str], *, cwd: Path) -> None:
    try:
        _run(
            [*base_args, "down", "--volumes", "--remove-orphans"],
            cwd=cwd,
            description="docker compose down",
            check=False,
        )
    except Exception as exc:  # pragma: no cover
        _LOG.warning("failed to tear down kafka fixture: %s", exc)


@fixture(name="kafka", replace=True, options=KafkaFixtureOptions)
def kafka() -> FixtureHandle:
    """Start a Kafka-compatible broker and seed it once for the benchmark."""

    context = current_context()
    if context is None:
        raise RuntimeError("kafka fixture requires an active benchmark context")
    options = current_options("kafka")
    if not isinstance(options, KafkaFixtureOptions):
        raise ValueError("invalid options for fixture 'kafka'")
    if not _compose_available():
        raise FixtureUnavailable("docker compose required but not found")
    if options.repetitions <= 0:
        raise ValueError("'kafka.repetitions' must be a positive integer")

    compose_file = _default_compose_file()
    if not compose_file.exists():
        raise RuntimeError(f"kafka compose file does not exist: {compose_file}")
    input_path = context.dataset_path
    if options.input_file is not None:
        input_path = _resolve_path(context.definition.path, options.input_file)
        if not input_path.exists():
            raise RuntimeError(f"kafka input file does not exist: {input_path}")

    cwd = compose_file.parent
    base_args = _compose_base_args(
        compose_file=compose_file,
        project_name=_project_name(context.definition.path),
    )
    _run(
        [*base_args, "up", "-d", options.service],
        cwd=cwd,
        description="docker compose up for kafka fixture",
    )
    _LOG.info("Started Kafka fixture service %s from %s", options.service, compose_file)
    _wait_for_cluster(
        base_args,
        cwd=cwd,
        service=options.service,
        timeout_seconds=options.wait_timeout_seconds,
        poll_interval_seconds=options.wait_poll_interval_seconds,
    )
    _LOG.info("Kafka fixture service %s is ready", options.service)
    _reset_topic(
        base_args,
        cwd=cwd,
        service=options.service,
        topic=options.topic,
        partitions=options.partitions,
        replication_factor=options.replication_factor,
    )
    _LOG.info("Reset Kafka topic %s", options.topic)
    _publish_dataset(
        base_args,
        cwd=cwd,
        service=options.service,
        topic=options.topic,
        input_path=input_path,
        repetitions=options.repetitions,
    )
    group_prefix = f"{_group_id(context.definition.path, options.topic)}-{uuid.uuid4().hex[:8]}"

    def _before_run(*, phase: str, run_index: int, env: dict[str, str], **_kwargs: object) -> None:
        env["BENCHMARK_KAFKA_GROUP_ID"] = f"{group_prefix}-{phase}-{run_index}"
        _LOG.info(
            "Using Kafka consumer group %s for %s run %s",
            env["BENCHMARK_KAFKA_GROUP_ID"],
            phase,
            run_index,
        )

    return FixtureHandle(
        env={
            "BENCHMARK_KAFKA_BOOTSTRAP_SERVERS": options.bootstrap_servers,
            "BENCHMARK_KAFKA_GROUP_ID": f"{group_prefix}-setup",
            "BENCHMARK_KAFKA_TOPIC": options.topic,
        },
        teardown=lambda: _teardown(base_args, cwd=cwd),
        hooks={"before_run": _before_run},
    )
