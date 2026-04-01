"""Node-backed fixtures for catalog lookup benchmarks."""

from __future__ import annotations

import json
import logging
import os
import re
import shutil
import signal
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass
from pathlib import Path

from tenzir_bench.fixtures import FixtureHandle, current_context, current_options, fixture

_LOG = logging.getLogger(__name__)
_ENDPOINT_RE = re.compile(r"^\d+\.\d+\.\d+\.\d+:\d+$")


@dataclass(frozen=True)
class NodeCatalogLookupOptions:
    """Configuration for the ``node_catalog_lookup`` benchmark fixture."""

    events: int = 100_000
    max_partition_size: int = 1
    schema: str = "suricata"
    query_hit_index: int = 50_000
    startup_timeout_seconds: float = 120.0
    shutdown_timeout_seconds: float = 20.0


def _resolve_binaries() -> tuple[Path, Path]:
    context = current_context()
    configured_tenzir = None if context is None else getattr(context, "tenzir_bin", None)
    if configured_tenzir is not None:
        tenzir_path = Path(configured_tenzir).resolve()
    else:
        tenzir_path = _resolve_tenzir_from_argv()
        if tenzir_path is None:
            tenzir = shutil.which("tenzir")
            if tenzir is None:
                raise RuntimeError(
                    "node_catalog_lookup fixture requires `tenzir` in PATH, "
                    "a benchmark context with a configured tenzir binary, "
                    "or a tenzir-bench invocation with --tenzir-bin",
                )
            tenzir_path = Path(tenzir).resolve()
    tenzir_node_path = tenzir_path.with_name("tenzir-node")
    if not tenzir_node_path.exists():
        fallback = shutil.which("tenzir-node")
        if fallback is None:
            raise RuntimeError(
                "node_catalog_lookup fixture requires `tenzir-node` in PATH "
                "or next to the `tenzir` binary",
            )
        tenzir_node_path = Path(fallback).resolve()
    return tenzir_path, tenzir_node_path


def _resolve_tenzir_from_argv() -> Path | None:
    args = sys.argv[1:]
    for index, arg in enumerate(args):
        if arg.startswith("--tenzir-bin="):
            value = arg.removeprefix("--tenzir-bin=").strip()
            if value:
                return Path(value).expanduser().resolve()
        if arg == "--tenzir-bin" and index + 1 < len(args):
            value = args[index + 1].strip()
            if value:
                return Path(value).expanduser().resolve()
    return None


def _suricata_dns_event(index: int) -> dict[str, object]:
    value = f"bench-{index:06}.example"
    return {
        "timestamp": "2026-01-01T00:00:00.000000Z",
        "flow_id": index + 1,
        "src_ip": "10.0.0.1",
        "src_port": 53000 + (index % 1000),
        "dest_ip": "10.0.0.53",
        "dest_port": 53,
        "proto": "UDP",
        "event_type": "dns",
        "dns": {
            "version": 2,
            "type": "query",
            "id": index + 1,
            "flags": "0120",
            "qr": False,
            "rd": True,
            "ra": False,
            "aa": False,
            "tc": False,
            "rrname": value,
            "rrtype": "A",
            "rcode": "NOERROR",
            "ttl": None,
            "tx_id": None,
            "grouped": None,
            "answers": None,
        },
    }


def _write_dataset(path: Path, events: int, query_hit_index: int) -> str:
    if events <= 0:
        raise ValueError("'node_catalog_lookup.events' must be positive")
    path.parent.mkdir(parents=True, exist_ok=True)
    _LOG.info("Materializing %s synthetic Suricata DNS events at %s", f"{events:,}", path)
    with path.open("w", encoding="utf-8") as handle:
        for index in range(events):
            handle.write(json.dumps(_suricata_dns_event(index), separators=(",", ":")))
            handle.write("\n")
    return f"bench-{query_hit_index:06}.example"


def _start_node(
    *,
    tenzir_node: Path,
    state_dir: Path,
    max_partition_size: int,
    startup_timeout_seconds: float,
    log_path: Path,
) -> tuple[subprocess.Popen[str], str]:
    state_dir.mkdir(parents=True, exist_ok=True)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    env.setdefault("TENZIR_CONSOLE_FORMAT", "none")
    with log_path.open("w", encoding="utf-8") as log_handle:
        process = subprocess.Popen(
            [
                str(tenzir_node),
                "-d",
                str(state_dir),
                "--endpoint=127.0.0.1:0",
                "--print-endpoint",
                "--max-partition-size",
                str(max_partition_size),
            ],
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            text=True,
            env=env,
        )
    deadline = time.monotonic() + startup_timeout_seconds
    endpoint = None
    offset = 0
    while time.monotonic() < deadline:
        with log_path.open(encoding="utf-8") as log_handle:
            log_handle.seek(offset)
            while True:
                line = log_handle.readline()
                if not line:
                    break
                offset = log_handle.tell()
                if _ENDPOINT_RE.match(line.strip()):
                    endpoint = line.strip()
                    break
        if endpoint is not None or process.poll() is not None:
            break
        time.sleep(0.1)
    if endpoint is None:
        process.kill()
        process.wait()
        detail = log_path.read_text(encoding="utf-8").strip() or "no output"
        raise RuntimeError(
            "node_catalog_lookup fixture failed to start tenzir-node and emit an endpoint: "
            f"{detail}",
        )
    _LOG.info("Started tenzir-node for catalog lookup benchmark at %s", endpoint)
    return process, endpoint


def _seed_node(
    *,
    tenzir: Path,
    endpoint: str,
    state_dir: Path,
    dataset_path: Path,
    schema: str,
    pipeline_path: Path,
) -> None:
    pipeline_path.write_text(
        "\n".join(
            [
                f'from_file "{dataset_path}" {{',
                f"  read_{schema}",
                "}",
                "import",
                "",
            ]
        ),
        encoding="utf-8",
    )
    env = os.environ.copy()
    env.setdefault("TENZIR_CONSOLE_FORMAT", "none")
    result = subprocess.run(
        [
            str(tenzir),
            "-e",
            endpoint,
            "-d",
            str(state_dir),
            "-f",
            str(pipeline_path),
        ],
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )
    if result.returncode == 0:
        _LOG.info("Seeded node at %s from %s", endpoint, dataset_path)
        return
    detail = (result.stderr or result.stdout or "").strip() or "no output"
    raise RuntimeError(f"failed to seed catalog lookup node: {detail}")


def _stop_node(
    process: subprocess.Popen[str],
    *,
    shutdown_timeout_seconds: float,
) -> None:
    if process.poll() is not None:
        return
    process.send_signal(signal.SIGINT)
    try:
        process.wait(timeout=shutdown_timeout_seconds)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()


@fixture(name="node_catalog_lookup", replace=True, options=NodeCatalogLookupOptions)
def node_catalog_lookup() -> FixtureHandle:
    """Start a node and seed it with one-event partitions for catalog lookups."""

    context = current_context()
    if context is None:
        raise RuntimeError("node_catalog_lookup fixture requires an active benchmark context")
    options = current_options("node_catalog_lookup")
    if not isinstance(options, NodeCatalogLookupOptions):
        raise ValueError("invalid options for fixture 'node_catalog_lookup'")
    if options.query_hit_index < 0 or options.query_hit_index >= options.events:
        raise ValueError("'node_catalog_lookup.query_hit_index' must refer to a generated event")
    tenzir, tenzir_node = _resolve_binaries()
    benchmark_root = context.output_root / "node-catalog-lookup" / uuid.uuid4().hex[:8]
    dataset_path = benchmark_root / "inputs" / "suricata-dns.ndjson"
    state_dir = benchmark_root / "state"
    import_pipeline_path = benchmark_root / "seed.tql"
    log_path = benchmark_root / "logs" / "node.log"
    query_value = _write_dataset(dataset_path, options.events, options.query_hit_index)
    process, endpoint = _start_node(
        tenzir_node=tenzir_node,
        state_dir=state_dir,
        max_partition_size=options.max_partition_size,
        startup_timeout_seconds=options.startup_timeout_seconds,
        log_path=log_path,
    )
    try:
        _seed_node(
            tenzir=tenzir,
            endpoint=endpoint,
            state_dir=state_dir,
            dataset_path=dataset_path,
            schema=options.schema,
            pipeline_path=import_pipeline_path,
        )
    except Exception:
        _stop_node(process, shutdown_timeout_seconds=options.shutdown_timeout_seconds)
        raise
    return FixtureHandle(
        env={
            "TENZIR_ENDPOINT": endpoint,
            "BENCHMARK_LOOKUP_VALUE": query_value,
        },
        teardown=lambda: _stop_node(
            process,
            shutdown_timeout_seconds=options.shutdown_timeout_seconds,
        ),
    )
