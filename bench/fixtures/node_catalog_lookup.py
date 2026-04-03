"""Node-backed fixtures for catalog lookup benchmarks."""

from __future__ import annotations

import json
import logging
import re
import signal
import subprocess
import time
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from typing import TextIO

from tenzir_bench.fixtures import (
    FixtureHandle,
    current_context,
    current_options,
    fixture,
)

_LOG = logging.getLogger(__name__)
_ENDPOINT_RE = re.compile(r"^\d+\.\d+\.\d+\.\d+:\d+$")


def _selected_input_name() -> str:
    context = current_context()
    if context is None:
        raise RuntimeError(
            "node_catalog_lookup fixture requires an active benchmark context"
        )
    fixture_spec = next(
        (
            fixture
            for fixture in context.definition.fixtures
            if fixture.name == "node_catalog_lookup"
        ),
        None,
    )
    selected_names = (
        fixture_spec.inputs
        if fixture_spec is not None and fixture_spec.inputs
        else context.definition.input_names
    )
    if len(selected_names) != 1:
        raise ValueError(
            "node_catalog_lookup expects exactly one selected input; set fixture.inputs accordingly"
        )
    return selected_names[0]


@dataclass(frozen=True)
class NodeCatalogLookupOptions:
    """Configuration for the ``node_catalog_lookup`` benchmark fixture."""

    max_partition_size: int = 1
    schema: str = "suricata"
    query_hit_index: int = 50_000
    startup_timeout_seconds: float = 120.0
    shutdown_timeout_seconds: float = 20.0


def _load_seed_event(path: Path) -> dict[str, object]:
    line = path.read_text(encoding="utf-8").splitlines()[0]
    payload = json.loads(line)
    if not isinstance(payload, dict):
        raise ValueError(f"seed dataset must start with a JSON object: {path}")
    return payload


def _suricata_dns_event(seed: dict[str, object], index: int) -> dict[str, object]:
    value = f"bench-{index:06}.example"
    event = deepcopy(seed)
    event["flow_id"] = index + 1
    event["src_port"] = 53_000 + (index % 1_000)
    dns = deepcopy(event.get("dns", {}))
    if not isinstance(dns, dict):
        dns = {}
    dns["id"] = index + 1
    dns["rrname"] = value
    event["dns"] = dns
    return event


def _write_dataset(
    path: Path, seed_path: Path, events: int, query_hit_index: int
) -> str:
    if events <= 0:
        raise ValueError("'benchmark.inputs.<name>.repetitions' must be positive")
    if query_hit_index < 0 or query_hit_index >= events:
        raise ValueError(
            "'node_catalog_lookup.query_hit_index' must refer to a generated event"
        )
    seed = _load_seed_event(seed_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    _LOG.info(
        "Materializing %s synthetic Suricata DNS events at %s", f"{events:,}", path
    )
    with path.open("w", encoding="utf-8") as handle:
        for index in range(events):
            handle.write(
                json.dumps(_suricata_dns_event(seed, index), separators=(",", ":"))
            )
            handle.write("\n")
    return f"bench-{query_hit_index:06}.example"


def _wait_for_endpoint(
    *,
    log_path: Path,
    process: subprocess.Popen[str],
    startup_timeout_seconds: float,
) -> str:
    deadline = time.monotonic() + startup_timeout_seconds
    endpoint: str | None = None
    offset = 0
    while time.monotonic() < deadline:
        with log_path.open(encoding="utf-8") as log_handle:
            log_handle.seek(offset)
            while True:
                line = log_handle.readline()
                if not line:
                    break
                offset = log_handle.tell()
                candidate = line.strip()
                if _ENDPOINT_RE.match(candidate):
                    endpoint = candidate
                    break
        if endpoint is not None or process.poll() is not None:
            break
        time.sleep(0.1)
    if endpoint is not None:
        return endpoint
    detail = log_path.read_text(encoding="utf-8").strip() or "no output"
    raise RuntimeError(
        f"node_catalog_lookup fixture failed to start tenzir-node and emit an endpoint: {detail}",
    )


def _start_node(
    *,
    state_dir: Path,
    log_path: Path,
    max_partition_size: int,
    startup_timeout_seconds: float,
) -> tuple[subprocess.Popen[str], TextIO, str]:
    context = current_context()
    if context is None:
        raise RuntimeError(
            "node_catalog_lookup fixture requires an active benchmark context"
        )
    state_dir.mkdir(parents=True, exist_ok=True)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_handle = log_path.open("w", encoding="utf-8")
    process: subprocess.Popen[str] | None = None
    try:
        process = context.runtime.popen_tenzir_node(
            args=[
                "-d",
                str(state_dir),
                "--endpoint=127.0.0.1:0",
                "--print-endpoint",
                "--max-partition-size",
                str(max_partition_size),
            ],
            env={"TENZIR_CONSOLE_FORMAT": "none"},
            stdout=log_handle,
            stderr=subprocess.STDOUT,
        )
        endpoint = _wait_for_endpoint(
            log_path=log_path,
            process=process,
            startup_timeout_seconds=startup_timeout_seconds,
        )
    except Exception:
        if process is not None:
            _stop_node(process, shutdown_timeout_seconds=5.0)
        log_handle.close()
        raise
    _LOG.info("Started tenzir-node for catalog lookup benchmark at %s", endpoint)
    return process, log_handle, endpoint


def _seed_node(
    *,
    endpoint: str,
    state_dir: Path,
    dataset_path: Path,
    schema: str,
    pipeline_path: Path,
) -> None:
    context = current_context()
    if context is None:
        raise RuntimeError(
            "node_catalog_lookup fixture requires an active benchmark context"
        )
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
    result = context.runtime.run_tenzir(
        args=[
            "-e",
            endpoint,
            "-d",
            str(state_dir),
            "-f",
            str(pipeline_path),
        ],
        env={"TENZIR_CONSOLE_FORMAT": "none"},
        capture_output=True,
        check=False,
        cwd=dataset_path.parent,
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
    """Start a node and seed it from the selected benchmark input."""

    context = current_context()
    if context is None:
        raise RuntimeError(
            "node_catalog_lookup fixture requires an active benchmark context"
        )
    options = current_options("node_catalog_lookup")
    if not isinstance(options, NodeCatalogLookupOptions):
        raise ValueError("invalid options for fixture 'node_catalog_lookup'")

    input_name = _selected_input_name()
    input_definition = context.definition.inputs[input_name]
    events = input_definition.repetitions
    query_value = f"bench-{options.query_hit_index:06}.example"
    if options.query_hit_index >= events:
        raise ValueError(
            "'node_catalog_lookup.query_hit_index' must refer to a generated event"
        )

    benchmark_root = context.output_root / "node-catalog-lookup"
    state_dir = benchmark_root / "state"
    import_pipeline_path = benchmark_root / "seed.tql"
    log_path = benchmark_root / "logs" / "node.log"
    process, log_handle, endpoint = _start_node(
        state_dir=state_dir,
        log_path=log_path,
        max_partition_size=options.max_partition_size,
        startup_timeout_seconds=options.startup_timeout_seconds,
    )

    def _seed(
        *,
        source_inputs: dict[str, Path],
        input_paths: dict[str, Path],
        **_kwargs: object,
    ) -> None:
        source_path = source_inputs[input_name]
        input_path = input_paths[input_name]
        _ = _write_dataset(
            input_path,
            source_path,
            events,
            options.query_hit_index,
        )
        _seed_node(
            endpoint=endpoint,
            state_dir=state_dir,
            dataset_path=input_path,
            schema=options.schema,
            pipeline_path=import_pipeline_path,
        )

    return FixtureHandle(
        env={
            "TENZIR_ENDPOINT": endpoint,
            "BENCHMARK_LOOKUP_VALUE": query_value,
        },
        teardown=lambda: _teardown_node_fixture(
            process=process,
            log_handle=log_handle,
            shutdown_timeout_seconds=options.shutdown_timeout_seconds,
        ),
        hooks={"seed": _seed},
    )


def _teardown_node_fixture(
    *,
    process: subprocess.Popen[str],
    log_handle: TextIO,
    shutdown_timeout_seconds: float,
) -> None:
    try:
        _stop_node(process, shutdown_timeout_seconds=shutdown_timeout_seconds)
    finally:
        log_handle.close()
