"""Iceberg REST fixture for the OCSF storage-layout benchmarks.

The fixture creates the target tables before every timed run, records their
schemas and snapshots, and validates the append afterwards. This keeps catalog
startup, table creation, and schema evolution outside the measured interval.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
import shutil
import socket
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from tenzir_bench.fixtures import (
    FixtureHandle,
    FixtureUnavailable,
    current_context,
    current_options,
    fixture,
)

_LOG = logging.getLogger(__name__)

_IMAGE = (
    "docker.io/apache/iceberg-rest-fixture"
    "@sha256:db8de90b5b7693d4ac334c336f91d9bbe320d7b19f4f514d26de84cdfbcbfe8d"
)
_EXPECTED_CLASS_UIDS = {
    1001,
    1002,
    1005,
    1006,
    1007,
    1008,
    2003,
    2004,
    2005,
    3001,
    3002,
    3003,
    3004,
    3006,
    4001,
    4002,
    4003,
    4004,
    4005,
    4006,
    4007,
    4008,
    4009,
    4010,
    5001,
}


@dataclass(frozen=True)
class IcebergFixtureOptions:
    """Configuration shared by the Iceberg benchmark definitions."""

    expected_events: int = 10_000_000
    expected_seed_events: int = 25
    batch_size: int = 65_536
    startup_timeout_seconds: float = 60.0


@dataclass(frozen=True)
class TableState:
    """Catalog state that must stay stable during an append."""

    schema_id: int
    schema_hash: str
    spec_id: int
    spec_hash: str
    snapshot_ids: frozenset[int]
    location: Path


@dataclass(frozen=True)
class TableTemplate:
    """Schema and partition specification for creating an empty table."""

    schema: dict[str, Any]
    partition_spec: dict[str, Any]


@dataclass
class RunState:
    """Per-run state captured after table setup and before measurement."""

    namespace: str
    tables: dict[str, TableState]
    storage: dict[str, int]
    prepared_input: Path
    expected_input_batches: int


# A benchmark process runs all selected definitions sequentially. Learning a
# wide OCSF schema requires one schema-evolution commit per class, so retain the
# result and create equivalent empty tables directly for later runs.
_TABLE_TEMPLATES: dict[str, dict[str, TableTemplate]] = {}
_PREPARED_INPUTS: dict[str, Path] = {}
_FILE_DIGESTS: dict[tuple[Path, int, int], str] = {}


def _container_runtime() -> str:
    for candidate in ("docker", "podman"):
        if shutil.which(candidate) is not None:
            return candidate
    raise FixtureUnavailable("container runtime (docker/podman) required but not found")


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _run(args: list[str], *, description: str) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(args, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        detail = (result.stderr or result.stdout or "").strip() or "no output"
        raise RuntimeError(f"{description} failed: {detail}")
    return result


def _request(
    catalog: str,
    path: str,
    *,
    method: str = "GET",
    json_body: dict[str, Any] | None = None,
    ignore_not_found: bool = False,
) -> Any:
    data = None
    headers: dict[str, str] = {}
    if json_body is not None:
        data = json.dumps(json_body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(
        f"{catalog}{path}", data=data, headers=headers, method=method
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            response_body = response.read()
    except urllib.error.HTTPError as exc:
        if ignore_not_found and exc.code == 404:
            return None
        detail = exc.read().decode("utf-8", errors="replace").strip()
        raise RuntimeError(
            f"Iceberg REST {method} {path} failed with HTTP {exc.code}: {detail}"
        ) from exc
    except (urllib.error.URLError, OSError) as exc:
        raise RuntimeError(f"Iceberg REST {method} {path} failed: {exc}") from exc
    if not response_body:
        return None
    return json.loads(response_body)


def _wait_for_catalog(catalog: str, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    last_error = "no response"
    while time.monotonic() < deadline:
        try:
            _request(catalog, "/v1/config")
            return
        except RuntimeError as exc:
            last_error = str(exc)
        time.sleep(0.5)
    raise RuntimeError(f"Iceberg REST catalog did not become ready: {last_error}")


def _create_namespace(catalog: str, namespace: str) -> None:
    _request(
        catalog,
        "/v1/namespaces",
        method="POST",
        json_body={"namespace": [namespace], "properties": {}},
    )


def _create_table(
    catalog: str,
    namespace: str,
    table: str,
    template: TableTemplate,
) -> None:
    escaped_namespace = urllib.parse.quote(namespace, safe="")
    _request(
        catalog,
        f"/v1/namespaces/{escaped_namespace}/tables",
        method="POST",
        json_body={
            "name": table,
            "schema": template.schema,
            "partition-spec": template.partition_spec,
            "properties": {},
        },
    )


def _definition_tags(definition: object) -> dict[str, str]:
    tags = getattr(definition, "tags", None)
    if not isinstance(tags, dict) or not all(
        isinstance(key, str) and isinstance(value, str) for key, value in tags.items()
    ):
        raise RuntimeError("Iceberg benchmark definition is missing string tags")
    return tags


def _repo_root(definition: object) -> Path:
    path = Path(getattr(definition, "path", Path.cwd())).resolve()
    for candidate in path.parents:
        if (candidate / ".git").exists() and (candidate / "bench").is_dir():
            return candidate
    raise RuntimeError(f"failed to find repository root from {path}")


def _validate_seed(path: Path, options: IcebergFixtureOptions) -> None:
    records = [
        json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()
    ]
    if len(records) != options.expected_seed_events:
        raise RuntimeError(
            f"expected {options.expected_seed_events} OCSF seeds, found {len(records)}"
        )
    class_uids = {record.get("class_uid") for record in records}
    if class_uids != _EXPECTED_CLASS_UIDS:
        raise RuntimeError(
            "unexpected OCSF class coverage: "
            f"expected {sorted(_EXPECTED_CLASS_UIDS)}, found {sorted(class_uids)}"
        )
    category_uids = {record.get("category_uid") for record in records}
    if category_uids != {1, 2, 3, 4, 5}:
        raise RuntimeError(
            f"expected OCSF categories 1 through 5, found {category_uids}"
        )
    versions = {
        record.get("metadata", {}).get("version")
        for record in records
        if isinstance(record.get("metadata"), dict)
    }
    if versions != {"1.8.0"}:
        raise RuntimeError(f"expected only OCSF 1.8.0 seeds, found {versions}")


def _namespace(definition: object, phase: str, run_index: int) -> str:
    definition_id = str(getattr(definition, "id", "iceberg"))
    stem = re.sub(r"[^a-z0-9]+", "_", definition_id.lower()).strip("_")
    return f"bench_{stem}_{phase}_{run_index}_{uuid.uuid4().hex[:8]}"


def _quoted(value: str) -> str:
    return json.dumps(value)


def _sink(
    table: str,
    *,
    mode: str,
    partition_by: str | None = None,
) -> str:
    parts = [
        f'to_iceberg {table}, catalog=env("ICEBERG_REST_URI"), mode="{mode}"',
    ]
    if partition_by is not None:
        parts.append(f"partition_by={partition_by}")
    parts.extend(
        [
            # Seed files are excluded from timing and storage deltas. Rotate
            # every seed slice so a snapshot opened before later schema
            # evolution cannot be committed with a stale timestamp.
            "max_size=1",
            "buffer_size=134217728",
            "timeout=1h",
        ]
    )
    return ", ".join(parts)


def _seed_pipeline(input_path: Path, namespace: str, tags: dict[str, str]) -> str:
    prefix = [
        f"from_file {_quoted(str(input_path))} {{ read_ndjson }}",
        "ocsf::cast encode_variants=true",
        "batch 65536",
    ]
    workload = tags.get("workload")
    if workload == "homogeneous":
        if tags.get("schema_width") == "class":
            class_uid = int(tags["class_uid"])
            prefix.insert(2, f"where class_uid == {class_uid}")
        prefix.append(
            _sink(
                _quoted(f"{namespace}.events"),
                mode="create",
            )
        )
        return "\n".join(prefix) + "\n"
    layout = tags.get("layout")
    if layout == "control":
        return ""
    if layout == "unified-unpartitioned":
        prefix.append(_sink(_quoted(f"{namespace}.events"), mode="create"))
    elif layout == "unified-partitioned":
        prefix.append(
            _sink(
                _quoted(f"{namespace}.events"),
                mode="create",
                partition_by="[class_uid, day(time)]",
            )
        )
    elif layout == "category-tables":
        prefix.extend(
            [
                "group category_uid {",
                "  "
                + _sink(
                    f'f"{namespace}.category_{{$group}}"',
                    mode="create",
                    partition_by="[class_uid, day(time)]",
                ),
                "}",
            ]
        )
    elif layout == "class-tables":
        prefix.extend(
            [
                "group class_uid {",
                "  "
                + _sink(
                    f'f"{namespace}.class_{{$group}}"',
                    mode="create",
                    partition_by="[day(time)]",
                ),
                "}",
            ]
        )
    else:
        raise RuntimeError(f"unsupported Iceberg benchmark layout: {layout!r}")
    return "\n".join(prefix) + "\n"


def _template_key(tags: dict[str, str]) -> str | None:
    if tags.get("workload") == "homogeneous":
        if tags.get("schema_width") == "unified":
            return "unified-unpartitioned"
        return f"class-{tags['class_uid']}-unpartitioned"
    layout = tags.get("layout")
    if layout == "control":
        return None
    return layout


def _file_digest(path: Path) -> str:
    resolved = path.resolve()
    stat = resolved.stat()
    key = (resolved, stat.st_size, stat.st_mtime_ns)
    cached = _FILE_DIGESTS.get(key)
    if cached is not None:
        return cached
    digest = hashlib.sha256()
    with resolved.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    result = digest.hexdigest()
    _FILE_DIGESTS[key] = result
    return result


def _prepared_input_key(
    tags: dict[str, str],
    options: IcebergFixtureOptions,
    dataset_path: Path,
    tenzir_path: Path,
) -> str:
    fingerprint = hashlib.sha256()
    for path in (dataset_path, tenzir_path, Path(__file__)):
        fingerprint.update(_file_digest(path).encode())
    return (
        f"ocsf-1.8-sparse-{tags.get('workload')}-"
        f"{tags.get('class_uid', 'all')}-{options.batch_size}-"
        f"{fingerprint.hexdigest()[:16]}"
    )


def _expected_input_batches(
    tags: dict[str, str],
    options: IcebergFixtureOptions,
) -> int:
    if tags.get("workload") == "homogeneous":
        return options.expected_events // options.batch_size
    prepared_events = options.expected_seed_events * options.batch_size
    return options.expected_seed_events * options.expected_events // prepared_events


def _prepare_input_pipeline(
    seed_path: Path,
    output_path: Path,
    tags: dict[str, str],
    repetitions: int,
    batch_size: int,
) -> str:
    pipeline = [
        f"from_file {_quoted(str(seed_path))} {{ read_ndjson }}",
        "ocsf::cast encode_variants=true",
    ]
    if tags.get("workload") == "homogeneous":
        pipeline.append(f"where class_uid == {int(tags['class_uid'])}")
    pipeline.extend(
        [
            f"repeat {repetitions}",
            "enumerate _benchmark_sequence",
            "time = (1735689600s + _benchmark_sequence * 1us).from_epoch()",
            'metadata.original_event_uid = f"iceberg-bench-{_benchmark_sequence}"',
            "drop _benchmark_sequence",
        ]
    )
    pipeline.extend(
        [
            # `repeat` interleaves the per-class seed events, and ordered
            # batching flushes on every schema change, which would degenerate
            # the cache into one single-event slice per input event. Relaxing
            # the ordering lets `batch` keep one buffer per schema and emit
            # full schema-homogeneous batches.
            f"unordered {{ batch {batch_size} }}",
            f"to_file {_quoted(str(output_path))} {{ write_bitz }}",
        ]
    )
    return "\n".join(pipeline) + "\n"


def _table_names(catalog: str, namespace: str) -> list[str]:
    escaped = urllib.parse.quote(namespace, safe="")
    response = _request(catalog, f"/v1/namespaces/{escaped}/tables")
    identifiers = response.get("identifiers", [])
    names = [identifier["name"] for identifier in identifiers]
    return sorted(names)


def _current_entry(metadata: dict[str, Any], plural: str, id_key: str) -> Any:
    current_id = metadata[id_key]
    for entry in metadata.get(plural, []):
        if entry.get("schema-id", entry.get("spec-id")) == current_id:
            return entry
    raise RuntimeError(f"Iceberg metadata has no {plural} entry for {current_id}")


def _canonical_hash(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _file_location(location: str) -> Path:
    parsed = urllib.parse.urlparse(location)
    if parsed.scheme != "file":
        raise RuntimeError(
            f"benchmark expected file-backed Iceberg table, got {location}"
        )
    return Path(urllib.parse.unquote(parsed.path))


def _table_state(catalog: str, namespace: str, table: str) -> TableState:
    escaped_namespace = urllib.parse.quote(namespace, safe="")
    escaped_table = urllib.parse.quote(table, safe="")
    response = _request(
        catalog,
        f"/v1/namespaces/{escaped_namespace}/tables/{escaped_table}",
    )
    metadata = response["metadata"]
    schema = _current_entry(metadata, "schemas", "current-schema-id")
    spec = _current_entry(metadata, "partition-specs", "default-spec-id")
    return TableState(
        schema_id=int(metadata["current-schema-id"]),
        schema_hash=_canonical_hash(schema),
        spec_id=int(metadata["default-spec-id"]),
        spec_hash=_canonical_hash(spec),
        snapshot_ids=frozenset(
            int(snapshot["snapshot-id"]) for snapshot in metadata.get("snapshots", [])
        ),
        location=_file_location(metadata["location"]),
    )


def _table_template(catalog: str, namespace: str, table: str) -> TableTemplate:
    escaped_namespace = urllib.parse.quote(namespace, safe="")
    escaped_table = urllib.parse.quote(table, safe="")
    response = _request(
        catalog,
        f"/v1/namespaces/{escaped_namespace}/tables/{escaped_table}",
    )
    metadata = response["metadata"]
    return TableTemplate(
        schema=_current_entry(metadata, "schemas", "current-schema-id"),
        partition_spec=_current_entry(
            metadata,
            "partition-specs",
            "default-spec-id",
        ),
    )


def _all_table_states(catalog: str, namespace: str) -> dict[str, TableState]:
    return {
        table: _table_state(catalog, namespace, table)
        for table in _table_names(catalog, namespace)
    }


def _storage_stats(locations: set[Path]) -> dict[str, int]:
    stats = {
        "data_bytes": 0,
        "data_files": 0,
        "metadata_bytes": 0,
        "metadata_files": 0,
    }
    for location in locations:
        if not location.exists():
            continue
        for path in location.rglob("*"):
            if not path.is_file():
                continue
            kind = "data" if path.suffix == ".parquet" else "metadata"
            stats[f"{kind}_bytes"] += path.stat().st_size
            stats[f"{kind}_files"] += 1
    return stats


def _snapshot_summaries(
    catalog: str,
    namespace: str,
    table: str,
    baseline: TableState,
) -> list[dict[str, Any]]:
    escaped_namespace = urllib.parse.quote(namespace, safe="")
    escaped_table = urllib.parse.quote(table, safe="")
    response = _request(
        catalog,
        f"/v1/namespaces/{escaped_namespace}/tables/{escaped_table}",
    )
    metadata = response["metadata"]
    return [
        snapshot
        for snapshot in metadata.get("snapshots", [])
        if int(snapshot["snapshot-id"]) not in baseline.snapshot_ids
    ]


def _summary_integer(snapshot: dict[str, Any], key: str) -> int:
    value = snapshot.get("summary", {}).get(key, 0)
    return int(value)


def _purge(catalog: str, namespace: str, tables: dict[str, TableState]) -> None:
    escaped_namespace = urllib.parse.quote(namespace, safe="")
    for table in tables:
        escaped_table = urllib.parse.quote(table, safe="")
        _request(
            catalog,
            f"/v1/namespaces/{escaped_namespace}/tables/{escaped_table}"
            "?purgeRequested=true",
            method="DELETE",
            ignore_not_found=True,
        )
    _request(
        catalog,
        f"/v1/namespaces/{escaped_namespace}",
        method="DELETE",
        ignore_not_found=True,
    )
    for state in tables.values():
        shutil.rmtree(state.location, ignore_errors=True)


@fixture(name="iceberg", replace=True, options=IcebergFixtureOptions)
def iceberg() -> FixtureHandle:
    """Run a local REST catalog and validate every measured Iceberg append."""

    context = current_context()
    if context is None:
        raise RuntimeError("iceberg fixture requires an active benchmark context")
    options = current_options("iceberg")
    if not isinstance(options, IcebergFixtureOptions):
        raise ValueError("invalid options for fixture 'iceberg'")
    if (
        options.expected_events <= 0
        or options.expected_seed_events <= 0
        or options.batch_size <= 0
    ):
        raise ValueError("Iceberg event counts and batch size must be positive")
    _validate_seed(context.dataset_path, options)
    tags = _definition_tags(context.definition)
    prepared_events = options.batch_size
    if tags.get("workload") != "homogeneous":
        prepared_events *= options.expected_seed_events
    stream_repetitions, remainder = divmod(options.expected_events, prepared_events)
    if remainder:
        raise ValueError(
            "Iceberg expected_events must be divisible by one prepared batch unit "
            f"({prepared_events} events)"
        )

    runtime = _container_runtime()
    port = _free_port()
    catalog = f"http://127.0.0.1:{port}"
    # The benchmark state root on macOS commonly contains "Application Support".
    # Arrow's file URI parser rejects the unescaped space returned by the REST
    # fixture, so keep the file warehouse in the git-ignored build tree. This
    # path is also visible to Docker benchmark targets through their worktree
    # mount.
    warehouse = (
        _repo_root(context.definition)
        / "build"
        / "tenzir-bench-iceberg"
        / uuid.uuid4().hex
    ).resolve()
    warehouse.mkdir(parents=True, exist_ok=True)
    container_name = f"tenzir-bench-iceberg-{uuid.uuid4().hex[:8]}"
    result = _run(
        [
            runtime,
            "run",
            "--rm",
            "--detach",
            "--name",
            container_name,
            "-p",
            f"{port}:8181",
            "-e",
            f"CATALOG_WAREHOUSE=file://{warehouse}",
            "-v",
            f"{warehouse}:{warehouse}",
            _IMAGE,
        ],
        description="start Iceberg REST catalog",
    )
    container_id = result.stdout.strip()
    try:
        _wait_for_catalog(catalog, options.startup_timeout_seconds)
    except Exception:
        subprocess.run([runtime, "stop", container_id], check=False)
        raise

    run_states: dict[tuple[str, int], RunState] = {}
    sidecar_root = context.output_root / "iceberg"
    setup_root = context.output_root / "iceberg-setup"
    sidecar_root.mkdir(parents=True, exist_ok=True)
    setup_root.mkdir(parents=True, exist_ok=True)

    def _before_run(
        *,
        definition: object,
        phase: str,
        run_index: int,
        env: dict[str, str],
        **_kwargs: object,
    ) -> None:
        tags = _definition_tags(definition)
        namespace = _namespace(definition, phase, run_index)
        env["BENCHMARK_ICEBERG_NAMESPACE"] = namespace
        prepared_key = _prepared_input_key(
            tags,
            options,
            context.dataset_path,
            context.runtime.tenzir_path,
        )
        prepared_input = _PREPARED_INPUTS.get(prepared_key)
        if prepared_input is None:
            prepared_root = _repo_root(definition) / "build" / "tenzir-bench-inputs"
            prepared_root.mkdir(parents=True, exist_ok=True)
            prepared_input = prepared_root / f"{prepared_key}.bitz"
            if not prepared_input.exists():
                temporary_input = prepared_input.with_suffix(f".{uuid.uuid4().hex}.tmp")
                prepare_path = setup_root / f"prepare-{prepared_key}.tql"
                prepare_path.write_text(
                    _prepare_input_pipeline(
                        context.dataset_path,
                        temporary_input,
                        tags,
                        options.batch_size,
                        options.batch_size,
                    ),
                    encoding="utf-8",
                )
                completed = context.runtime.run_tenzir(
                    args=["-f", str(prepare_path)],
                    env={**env, "TENZIR_CONSOLE_FORMAT": "plain"},
                    capture_output=True,
                    check=False,
                )
                if completed.returncode != 0:
                    temporary_input.unlink(missing_ok=True)
                    detail = (
                        completed.stderr or completed.stdout or ""
                    ).strip() or "no output"
                    raise RuntimeError(f"failed to prepare typed OCSF input: {detail}")
                temporary_input.replace(prepared_input)
            _PREPARED_INPUTS[prepared_key] = prepared_input
        env["BENCHMARK_ICEBERG_INPUT_PATH"] = str(prepared_input)
        expected_batches = _expected_input_batches(tags, options)
        template_key = _template_key(tags)
        templates = _TABLE_TEMPLATES.get(template_key) if template_key else None
        if templates is not None:
            _create_namespace(catalog, namespace)
            for table, template in templates.items():
                _create_table(catalog, namespace, table, template)
            tables = _all_table_states(catalog, namespace)
            run_states[(phase, run_index)] = RunState(
                namespace=namespace,
                tables=tables,
                storage=_storage_stats({state.location for state in tables.values()}),
                prepared_input=prepared_input,
                expected_input_batches=expected_batches,
            )
            return
        # Use the compact schema corpus for setup. The prepared BITZ input has
        # the same schema coverage but would needlessly dominate untimed setup.
        pipeline = _seed_pipeline(context.dataset_path, namespace, tags)
        if not pipeline:
            run_states[(phase, run_index)] = RunState(
                namespace=namespace,
                tables={},
                storage=_storage_stats(set()),
                prepared_input=prepared_input,
                expected_input_batches=expected_batches,
            )
            return
        # Grouped table writers otherwise race while auto-creating the same
        # namespace and its properties.
        _create_namespace(catalog, namespace)
        setup_path = setup_root / f"{namespace}.tql"
        setup_path.write_text(pipeline, encoding="utf-8")
        setup_env = {
            **env,
            "ICEBERG_REST_URI": catalog,
            "TENZIR_CONSOLE_FORMAT": "plain",
        }
        completed = context.runtime.run_tenzir(
            args=["-f", str(setup_path)],
            env=setup_env,
            capture_output=True,
            check=False,
        )
        if completed.returncode != 0:
            detail = (completed.stderr or completed.stdout or "").strip() or "no output"
            raise RuntimeError(f"failed to pre-seed Iceberg tables: {detail}")
        tables = _all_table_states(catalog, namespace)
        expected_tables = int(tags["expected_tables"])
        if len(tables) != expected_tables:
            _purge(catalog, namespace, tables)
            raise RuntimeError(
                f"Iceberg setup created {len(tables)} tables, expected {expected_tables}"
            )
        if template_key is not None:
            _TABLE_TEMPLATES[template_key] = {
                table: _table_template(catalog, namespace, table) for table in tables
            }
        run_states[(phase, run_index)] = RunState(
            namespace=namespace,
            tables=tables,
            storage=_storage_stats({state.location for state in tables.values()}),
            prepared_input=prepared_input,
            expected_input_batches=expected_batches,
        )

    def _after_run(
        *,
        definition: object,
        phase: str,
        run_index: int,
        success: bool,
        **_kwargs: object,
    ) -> None:
        state = run_states.pop((phase, run_index))
        tags = _definition_tags(definition)
        if tags.get("layout") == "control":
            payload = {
                "phase": phase,
                "run_index": run_index,
                "namespace": state.namespace,
                "success": success,
                "expected_records": options.expected_events,
                "expected_input_batches": state.expected_input_batches,
                "expected_parquet_row_groups": state.expected_input_batches,
                "prepared_input_bytes": state.prepared_input.stat().st_size,
                "tables": [],
                "added_records": 0,
            }
            definition_root = sidecar_root / str(getattr(definition, "id"))
            definition_root.mkdir(parents=True, exist_ok=True)
            (definition_root / f"{phase}-{run_index}.json").write_text(
                json.dumps(payload, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            return
        observed: dict[str, TableState] = {}
        error: str | None = None
        payload: dict[str, Any] = {
            "phase": phase,
            "run_index": run_index,
            "namespace": state.namespace,
            "success": success,
            "expected_records": options.expected_events,
            "expected_input_batches": state.expected_input_batches,
            "expected_parquet_row_groups": state.expected_input_batches,
            "prepared_input_bytes": state.prepared_input.stat().st_size,
        }
        try:
            if not success:
                return
            observed = _all_table_states(catalog, state.namespace)
            expected_tables = int(tags["expected_tables"])
            if len(observed) != expected_tables:
                raise RuntimeError(
                    f"Iceberg append left {len(observed)} tables, expected {expected_tables}"
                )
            if observed.keys() != state.tables.keys():
                raise RuntimeError(
                    f"Iceberg table set changed: {sorted(state.tables)} -> {sorted(observed)}"
                )
            table_summaries: dict[str, Any] = {}
            added_records = 0
            added_data_files = 0
            added_files_size = 0
            for table, baseline in state.tables.items():
                current = observed[table]
                if (current.schema_id, current.schema_hash) != (
                    baseline.schema_id,
                    baseline.schema_hash,
                ):
                    raise RuntimeError(
                        f"Iceberg schema changed during append for {table}"
                    )
                if (current.spec_id, current.spec_hash) != (
                    baseline.spec_id,
                    baseline.spec_hash,
                ):
                    raise RuntimeError(
                        f"Iceberg partition spec changed during append for {table}"
                    )
                snapshots = _snapshot_summaries(
                    catalog,
                    state.namespace,
                    table,
                    baseline,
                )
                records = sum(
                    _summary_integer(item, "added-records") for item in snapshots
                )
                data_files = sum(
                    _summary_integer(item, "added-data-files") for item in snapshots
                )
                files_size = sum(
                    _summary_integer(item, "added-files-size") for item in snapshots
                )
                added_records += records
                added_data_files += data_files
                added_files_size += files_size
                table_summaries[table] = {
                    "added_records": records,
                    "added_data_files": data_files,
                    "added_files_size": files_size,
                    "new_snapshot_ids": [
                        int(item["snapshot-id"]) for item in snapshots
                    ],
                    "schema_id": current.schema_id,
                    "partition_spec_id": current.spec_id,
                }
            if added_records != options.expected_events:
                raise RuntimeError(
                    f"Iceberg append added {added_records} records, "
                    f"expected {options.expected_events}"
                )
            storage = _storage_stats({item.location for item in observed.values()})
            payload.update(
                {
                    "tables": table_summaries,
                    "added_records": added_records,
                    "added_data_files": added_data_files,
                    "added_files_size": added_files_size,
                    "storage_delta": {
                        key: storage[key] - state.storage[key] for key in storage
                    },
                }
            )
        except Exception as exc:
            error = str(exc)
            payload["validation_error"] = error
        finally:
            cleanup_tables = observed or state.tables
            try:
                _purge(catalog, state.namespace, cleanup_tables)
            except Exception as exc:
                _LOG.warning(
                    "failed to purge Iceberg namespace %s: %s", state.namespace, exc
                )
            definition_root = sidecar_root / str(getattr(definition, "id"))
            definition_root.mkdir(parents=True, exist_ok=True)
            (definition_root / f"{phase}-{run_index}.json").write_text(
                json.dumps(payload, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
        if error is not None:
            raise RuntimeError(error)

    def _teardown() -> None:
        result = subprocess.run(
            [runtime, "stop", container_id],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode != 0:
            detail = (result.stderr or result.stdout or "").strip() or "no output"
            _LOG.warning("failed to stop Iceberg REST catalog: %s", detail)
        shutil.rmtree(warehouse, ignore_errors=True)

    return FixtureHandle(
        env={
            "ICEBERG_REST_URI": catalog,
            "ICEBERG_WAREHOUSE_DIR": str(warehouse),
            "ICEBERG_REST_CONTAINER_ID": container_id,
            "ICEBERG_REST_CONTAINER_RUNTIME": runtime,
            "BENCHMARK_ICEBERG_STREAM_REPETITIONS": str(stream_repetitions),
        },
        teardown=_teardown,
        hooks={"before_run": _before_run, "after_run": _after_run},
    )


__all__ = ["IcebergFixtureOptions", "iceberg"]
