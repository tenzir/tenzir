#!/usr/bin/env python3
"""Run and compare `tenzir-bench` benchmark suites for CI."""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from collections.abc import Sequence

import boto3
from botocore.exceptions import ClientError
from tenzir_bench.compare import _resolve_entry
from tenzir_bench.executor import BenchmarkExecutor
from tenzir_bench.paths import BenchPaths
from tenzir_bench.pr_comment import render_grouped_markdown
from tenzir_bench.publisher import _parse_destination
from tenzir_bench.reports import Report, load_reports, select_fastest
from tenzir_bench.runners import RunnerRegistry
from tenzir_bench.specs import load_definitions_from_paths


@dataclass(frozen=True)
class BuildSpec:
    label: str
    target: str
    kind: str
    path: str | None = None
    image: str | None = None
    storage_prefix: str | None = None


def load_build_spec(path: Path) -> BuildSpec:
    payload = json.loads(path.read_text(encoding="utf-8"))
    label = payload.get("label") or payload.get("target") or path.stem
    if not isinstance(label, str):
        raise RuntimeError(f"{path}: build label must be a string")
    target = payload.get("target")
    kind = payload.get("kind")
    if not isinstance(target, str) or not isinstance(kind, str):
        raise RuntimeError(f"{path}: missing target or kind")
    entry_path = payload.get("path")
    image = payload.get("image")
    storage_prefix = payload.get("storage_prefix")
    if entry_path is not None and not isinstance(entry_path, str):
        raise RuntimeError(f"{path}: path must be a string")
    if image is not None and not isinstance(image, str):
        raise RuntimeError(f"{path}: image must be a string")
    if storage_prefix is not None and not isinstance(storage_prefix, str):
        raise RuntimeError(f"{path}: storage_prefix must be a string")
    return BuildSpec(
        label=label,
        target=target,
        kind=kind,
        path=entry_path,
        image=image,
        storage_prefix=storage_prefix,
    )


def resolve_build_entry(paths: BenchPaths, build: BuildSpec) -> Path:
    if build.kind == "docker":
        if not build.image:
            raise RuntimeError(f"{build.label}: docker build is missing an image ref")
        return _resolve_entry(paths, f"docker://{build.image}")
    if build.kind == "static":
        if not build.path:
            raise RuntimeError(f"{build.label}: static build is missing a binary path")
        return _resolve_entry(paths, build.path)
    raise RuntimeError(f"{build.label}: unsupported build kind {build.kind}")


def load_contexts(
    executor: BenchmarkExecutor,
    bench_root: Path,
    *,
    benchmarks: Sequence[str] | None = None,
) -> list:
    definition_paths = [bench_root / benchmark for benchmark in benchmarks] if benchmarks else [bench_root]
    definitions = load_definitions_from_paths(
        definition_paths,
        version_supplier=lambda: executor.build_info().version,
        root=bench_root.parent,
    )
    contexts = []
    for definition in definitions:
        context = executor.create_context(definition)
        if context is not None:
            contexts.append(context)
    return contexts


def run_local_build(
    build: BuildSpec,
    *,
    bench_root: Path,
    paths: BenchPaths,
    benchmarks: Sequence[str] | None = None,
    force: bool = True,
) -> dict[str, Report]:
    binary = resolve_build_entry(paths, build)
    executor = BenchmarkExecutor(paths, binary, RunnerRegistry())
    contexts = load_contexts(executor, bench_root, benchmarks=benchmarks)
    if not contexts:
        return {}
    safe_label = build.label.replace(" ", "-").replace("/", "-")
    output_dir = paths.results_state_dir / "benchmark-ci" / safe_label
    executor.ensure_reports(contexts, output_dir, force=force)
    return select_fastest(load_reports(output_dir))


def normalize_reports(reports: dict[str, Report]) -> dict[tuple[str, str], Report]:
    normalized: dict[tuple[str, str], Report] = {}
    for report in reports.values():
        implementation_id = report.implementation_id or report.pipeline
        normalized[(report.benchmark_id, implementation_id)] = report
    return normalized


def publish_reports(reports: dict[str, Report], *, destination: str) -> None:
    bucket, prefix = _parse_destination(destination, default_bucket="")
    s3 = boto3.client("s3")
    normalized = normalize_reports(reports)
    for (benchmark_id, implementation_id), report in normalized.items():
        key = str(Path(prefix) / benchmark_id / implementation_id / "report.json")
        try:
            s3.head_object(Bucket=bucket, Key=key)
            continue
        except ClientError as exc:  # type: ignore[assignment]
            if exc.response["Error"]["Code"] != "404":
                raise
        s3.upload_file(str(report.path), bucket, key)


def download_reference_reports(
    *,
    destination: str,
    benchmarks: Sequence[str] | None = None,
) -> dict[str, Report]:
    bucket, prefix = _parse_destination(destination, default_bucket="")
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    reports: dict[str, Report] = {}
    selected = set(benchmarks or [])
    for page in paginator.paginate(Bucket=bucket, Prefix=str(prefix)):
        for obj in page.get("Contents", []):
            key = obj.get("Key")
            if not isinstance(key, str) or not key.endswith(".json"):
                continue
            body = s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")
            payload = json.loads(body)
            pipeline = payload["pipeline"]
            benchmark_id = str(payload.get("benchmark_id") or pipeline)
            if selected and benchmark_id not in selected:
                continue
            implementation_id = payload.get("implementation_id")
            report = Report(
                path=Path(f"s3://{bucket}/{key}"),
                pipeline=pipeline,
                benchmark_id=benchmark_id,
                implementation_id=implementation_id,
                wall_clock=float(payload["runtime"]["wall_clock"]),
                rss_kb=int(payload["runtime"]["max_resident_set_kb"]),
                build_version=payload.get("build", {}).get("version"),
                artifact_id=None,
            )
            reports[pipeline] = report
    return reports


def filter_missing_reports(
    local_reports: dict[str, Report],
    remote_reports: dict[str, Report],
) -> list[str]:
    required = normalize_reports(local_reports)
    available = normalize_reports(remote_reports)
    missing: list[str] = []
    for key, report in required.items():
        if key not in available:
            missing.append(report.pipeline)
    return missing


def render_markdown_for_builds(builds: list[tuple[str, dict[str, Report]]]) -> str:
    return render_grouped_markdown(builds)


def cmd_reference(args: argparse.Namespace) -> int:
    paths = BenchPaths.create()
    bench_root = Path(args.bench_root).resolve()
    build = load_build_spec(Path(args.build))
    reports = run_local_build(
        build,
        bench_root=bench_root,
        paths=paths,
        benchmarks=args.benchmark,
    )
    if args.destination:
        publish_reports(reports, destination=args.destination)
    return 0


def cmd_compare(args: argparse.Namespace) -> int:
    paths = BenchPaths.create()
    bench_root = Path(args.bench_root).resolve()
    build_reports: list[tuple[str, dict[str, Report]]] = []
    for build_path in args.build:
        build = load_build_spec(Path(build_path))
        reports: dict[str, Report]
        if build.storage_prefix:
            reports = download_reference_reports(
                destination=build.storage_prefix,
                benchmarks=args.benchmark,
            )
            probe_reports = run_local_build(
                build,
                bench_root=bench_root,
                paths=paths,
                benchmarks=args.benchmark,
            )
            if filter_missing_reports(local_reports=probe_reports, remote_reports=reports):
                publish_reports(probe_reports, destination=build.storage_prefix)
                reports = download_reference_reports(
                    destination=build.storage_prefix,
                    benchmarks=args.benchmark,
                )
        else:
            reports = run_local_build(
                build,
                bench_root=bench_root,
                paths=paths,
                benchmarks=args.benchmark,
            )
        build_reports.append((build.label, reports))

    markdown = render_markdown_for_builds(build_reports)
    if args.markdown_output:
        Path(args.markdown_output).write_text(markdown, encoding="utf-8")
    else:
        print(markdown)
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    reference = subparsers.add_parser("reference")
    reference.add_argument("--bench-root", default="bench", help="Benchmark root directory")
    reference.add_argument("--build", required=True, help="Path to the resolved build spec JSON")
    reference.add_argument("--benchmark", action="append", help="Benchmark id to run. Repeat to select multiple.")
    reference.add_argument("--destination", help="S3 destination for normalized reference reports")
    reference.set_defaults(func=cmd_reference)

    compare = subparsers.add_parser("compare")
    compare.add_argument("--bench-root", default="bench", help="Benchmark root directory")
    compare.add_argument(
        "--build",
        action="append",
        required=True,
        help="Path to a build spec JSON. Repeat in display order.",
    )
    compare.add_argument("--benchmark", action="append", help="Benchmark id to run. Repeat to select multiple.")
    compare.add_argument("--markdown-output", help="Write grouped markdown to this file")
    compare.set_defaults(func=cmd_compare)

    args = parser.parse_args()
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
