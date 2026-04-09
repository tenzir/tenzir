#!/usr/bin/env python3
"""Run and compare `tenzir-bench` benchmark suites for CI."""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import tarfile
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

from tenzir_bench.compare import (
    CompareBuild,
    expected_report_identities,
    prepare_compare_reports_for_build,
    resolve_entry,
)
from tenzir_bench.executor import BenchmarkExecutor
from tenzir_bench.hardware import current_hardware_key
from tenzir_bench.paths import BenchPaths
from tenzir_bench.pr_comment import BuildDisplay, render_grouped_markdown
from tenzir_bench.publisher import Publisher
from tenzir_bench.references import (
    download_reference_reports as load_reference_reports,
    missing_report_identities,
    normalize_reports_by_identity,
)
from tenzir_bench.reports import Report
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
    version: str | None = None
    run_id: int | None = None
    artifact_name: str | None = None
    role: str | None = None
    ref: str | None = None
    implicit: bool = False
    request_index: int | None = None


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
    version = payload.get("version")
    run_id = payload.get("run_id")
    artifact_name = payload.get("artifact_name")
    role = payload.get("role")
    ref = payload.get("ref")
    implicit = payload.get("implicit", False)
    request_index = payload.get("request_index")
    if entry_path is not None and not isinstance(entry_path, str):
        raise RuntimeError(f"{path}: path must be a string")
    if image is not None and not isinstance(image, str):
        raise RuntimeError(f"{path}: image must be a string")
    if storage_prefix is not None and not isinstance(storage_prefix, str):
        raise RuntimeError(f"{path}: storage_prefix must be a string")
    if version is not None and not isinstance(version, str):
        raise RuntimeError(f"{path}: version must be a string")
    if run_id is not None and not isinstance(run_id, int):
        raise RuntimeError(f"{path}: run_id must be an integer")
    if artifact_name is not None and not isinstance(artifact_name, str):
        raise RuntimeError(f"{path}: artifact_name must be a string")
    if role is not None and not isinstance(role, str):
        raise RuntimeError(f"{path}: role must be a string")
    if ref is not None and not isinstance(ref, str):
        raise RuntimeError(f"{path}: ref must be a string")
    if not isinstance(implicit, bool):
        raise RuntimeError(f"{path}: implicit must be a boolean")
    if request_index is not None and not isinstance(request_index, int):
        raise RuntimeError(f"{path}: request_index must be an integer")
    return BuildSpec(
        label=label,
        target=target,
        kind=kind,
        path=entry_path,
        image=image,
        storage_prefix=storage_prefix,
        version=version,
        run_id=run_id,
        artifact_name=artifact_name,
        role=role,
        ref=ref,
        implicit=implicit,
        request_index=request_index,
    )


def infer_build_spec(target: str, *, scratch_dir: Path) -> BuildSpec:
    path = Path(target).expanduser()
    if not path.exists():
        return BuildSpec(label=target, target="docker", kind="docker", image=target)
    resolved = path.resolve()
    if resolved.is_file() and resolved.name == "tenzir":
        return BuildSpec(
            label=resolved.name, target="static", kind="static", path=str(resolved)
        )
    if resolved.is_dir():
        binary = next(iter(sorted(resolved.rglob("bin/tenzir"))), None)
        if binary is not None:
            return BuildSpec(
                label=resolved.name, target="static", kind="static", path=str(binary)
            )
        tarballs = sorted(resolved.rglob("*.tar.gz"))
        if tarballs:
            extracted_dir = scratch_dir / "static-extracted"
            extracted_dir.mkdir(parents=True, exist_ok=True)
            with tarfile.open(tarballs[0], "r:gz") as archive:
                archive.extractall(extracted_dir)
            binary = next(iter(sorted(extracted_dir.rglob("bin/tenzir"))), None)
            if binary is None:
                raise RuntimeError(
                    f"{resolved}: failed to locate bin/tenzir after extracting {tarballs[0]}"
                )
            return BuildSpec(
                label=resolved.name, target="static", kind="static", path=str(binary)
            )
    if resolved.is_file() and resolved.suffixes[-2:] == [".tar", ".gz"]:
        extracted_dir = scratch_dir / "static-extracted"
        extracted_dir.mkdir(parents=True, exist_ok=True)
        with tarfile.open(resolved, "r:gz") as archive:
            archive.extractall(extracted_dir)
        binary = next(iter(sorted(extracted_dir.rglob("bin/tenzir"))), None)
        if binary is None:
            raise RuntimeError(
                f"{resolved}: failed to locate bin/tenzir after extracting archive"
            )
        return BuildSpec(
            label=resolved.stem, target="static", kind="static", path=str(binary)
        )
    raise RuntimeError(
        f"failed to infer benchmark target kind from {target}; expected a docker image ref, "
        "a tenzir binary, or a static build artifact directory/archive",
    )


def resolve_build_entry(paths: BenchPaths, build: BuildSpec) -> Path:
    if build.kind == "docker":
        if not build.image:
            raise RuntimeError(f"{build.label}: docker build is missing an image ref")
        return resolve_entry(paths, f"docker://{build.image}")
    if build.kind == "static":
        if build.path:
            return resolve_entry(paths, build.path)
        return resolve_entry(paths, str(_materialize_static_artifact(paths, build)))
    raise RuntimeError(f"{build.label}: unsupported build kind {build.kind}")


def _selected_benchmark_paths(
    bench_root: Path,
    benchmarks: Sequence[str] | None = None,
) -> list[Path]:
    benchmarks_root = bench_root / "benchmarks"
    if benchmarks:
        return [benchmarks_root / benchmark for benchmark in benchmarks]
    return [bench_root]


def _to_compare_build(
    paths: BenchPaths,
    build: BuildSpec,
    *,
    materialize_static: bool,
) -> CompareBuild:
    binary: Path | None = None
    if build.kind == "docker":
        if not build.image:
            raise RuntimeError(f"{build.label}: docker build is missing an image ref")
        binary = resolve_entry(paths, f"docker://{build.image}")
    elif build.kind == "static":
        if build.path:
            binary = resolve_entry(paths, build.path)
        elif materialize_static:
            binary = resolve_entry(
                paths, str(_materialize_static_artifact(paths, build))
            )
    else:
        raise RuntimeError(f"{build.label}: unsupported build kind {build.kind}")
    return CompareBuild(
        label=build.label,
        binary=binary,
        force=True,
        reference_destination=build.storage_prefix,
        target=build.target,
        version=build.version,
    )


def _materialize_static_artifact(paths: BenchPaths, build: BuildSpec) -> Path:
    if build.run_id is None or not build.artifact_name:
        raise RuntimeError(
            f"{build.label}: static build is missing both a binary path and artifact download metadata",
        )
    repo = os.getenv("GITHUB_REPOSITORY", "tenzir/tenzir")
    safe_label = re.sub(r"[^A-Za-z0-9._-]", "-", build.label)
    artifact_root = (
        paths.results_state_dir
        / "benchmark-ci"
        / "_artifacts"
        / f"{safe_label}-{build.run_id}"
    )
    binary = next(iter(sorted(artifact_root.rglob("bin/tenzir"))), None)
    if binary is not None:
        return binary
    download_dir = artifact_root / "download"
    if not download_dir.exists():
        download_dir.mkdir(parents=True, exist_ok=True)
        subprocess.run(
            [
                "gh",
                "run",
                "download",
                str(build.run_id),
                "--repo",
                repo,
                "--name",
                build.artifact_name,
                "--dir",
                str(download_dir),
            ],
            check=True,
            capture_output=True,
            text=True,
        )
    tarballs = sorted(download_dir.rglob("*.tar.gz"))
    if not tarballs:
        raise RuntimeError(
            f"{build.label}: no tarball found in downloaded artifact {build.artifact_name}"
        )
    extracted_dir = artifact_root / "extract"
    if not extracted_dir.exists():
        extracted_dir.mkdir(parents=True, exist_ok=True)
        with tarfile.open(tarballs[0], "r:gz") as archive:
            archive.extractall(extracted_dir)
    binary = next(iter(sorted(extracted_dir.rglob("bin/tenzir"))), None)
    if binary is None:
        raise RuntimeError(
            f"{build.label}: failed to locate bin/tenzir after extracting {tarballs[0]}"
        )
    return binary


def load_contexts(
    executor: BenchmarkExecutor,
    bench_root: Path,
    *,
    benchmarks: Sequence[str] | None = None,
) -> list:
    benchmarks_root = bench_root / "benchmarks"
    definition_paths = (
        [benchmarks_root / benchmark for benchmark in benchmarks]
        if benchmarks
        else [bench_root]
    )
    definitions = load_definitions_from_paths(
        definition_paths,
        version_supplier=lambda: executor.build_info().version,
        root=bench_root,
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
    compare_build = _to_compare_build(paths, build, materialize_static=True)
    if not force:
        compare_build = CompareBuild(
            label=compare_build.label,
            binary=compare_build.binary,
            force=False,
            tenzir_args=compare_build.tenzir_args,
            reference_destination=compare_build.reference_destination,
            target=compare_build.target,
            version=compare_build.version,
        )
    return prepare_compare_reports_for_build(
        paths,
        compare_build,
        _selected_benchmark_paths(bench_root, benchmarks),
        compare_root=paths.results_state_dir / "benchmark-ci",
        registry=RunnerRegistry(),
        validate=False,
        dry_run=False,
        verbose=False,
    )


def normalize_reports(reports: dict[str, Report]) -> dict[tuple[str, str], Report]:
    return normalize_reports_by_identity(reports)


def publish_reports(reports: dict[str, Report], *, destination: str) -> None:
    Publisher().publish_reports(reports, destination)


def download_reference_reports(
    *,
    destination: str,
    benchmarks: Sequence[str] | None = None,
    hardware_key: str,
) -> dict[str, Report]:
    return {
        f"{benchmark_id}/{implementation_id}": report
        for (benchmark_id, implementation_id), report in load_reference_reports(
            destination,
            benchmarks=benchmarks,
            hardware_key=hardware_key,
            default_bucket="",
        ).items()
    }


def filter_missing_reports(
    expected: set[tuple[str, str]],
    remote_reports: dict[str, Report],
) -> set[tuple[str, str]]:
    return missing_report_identities(expected, remote_reports)


def render_markdown_for_builds(
    builds: Sequence[tuple[BuildSpec, dict[str, Report]]],
) -> str:
    rendered = [
        BuildDisplay(
            label=build.label,
            reports=reports,
            role=build.role
            if build.role in {"release", "main", "extra", "candidate"}
            else "extra",
            target=build.target,
            ref=build.ref,
            implicit=build.implicit,
            request_index=build.request_index,
        )
        for build, reports in builds
    ]
    return render_grouped_markdown(rendered)


def cmd_reference(args: argparse.Namespace) -> int:
    paths = BenchPaths.create()
    bench_root = Path(args.bench_root).resolve()
    if args.build:
        build = load_build_spec(Path(args.build))
    else:
        build = infer_build_spec(
            args.target,
            scratch_dir=paths.results_state_dir / "benchmark-ci" / "_resolved-target",
        )
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
    benchmark_dirs = _selected_benchmark_paths(bench_root, args.benchmark)
    build_reports: list[tuple[BuildSpec, dict[str, Report]]] = []
    compare_root = paths.results_state_dir / "benchmark-ci" / "_compare"
    compare_root.mkdir(parents=True, exist_ok=True)
    for build_path in args.build:
        build = load_build_spec(Path(build_path))
        if (
            build.kind == "static"
            and build.storage_prefix is not None
            and build.path is None
            and build.run_id is not None
            and build.artifact_name is not None
        ):
            preflight_build = _to_compare_build(paths, build, materialize_static=False)
            expected = expected_report_identities(
                paths, preflight_build, benchmark_dirs
            )
            remote_reports = download_reference_reports(
                destination=build.storage_prefix,
                benchmarks=args.benchmark,
                hardware_key=current_hardware_key(),
            )
            if filter_missing_reports(expected=expected, remote_reports=remote_reports):
                compare_build = _to_compare_build(paths, build, materialize_static=True)
            else:
                compare_build = preflight_build
        else:
            compare_build = _to_compare_build(paths, build, materialize_static=False)
        reports = prepare_compare_reports_for_build(
            paths,
            compare_build,
            benchmark_dirs,
            compare_root=compare_root,
            registry=RunnerRegistry(),
            validate=False,
            dry_run=False,
            verbose=False,
        )
        build_reports.append((build, reports))

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
    reference.add_argument(
        "--bench-root", default="bench", help="Benchmark root directory"
    )
    reference_inputs = reference.add_mutually_exclusive_group(required=True)
    reference_inputs.add_argument(
        "--build", help="Path to the resolved build spec JSON"
    )
    reference_inputs.add_argument(
        "--target",
        help="Docker image ref, static artifact directory/archive, or tenzir binary",
    )
    reference.add_argument(
        "--benchmark",
        action="append",
        help="Benchmark id to run. Repeat to select multiple.",
    )
    reference.add_argument(
        "--destination", help="S3 destination for normalized reference reports"
    )
    reference.set_defaults(func=cmd_reference)

    compare = subparsers.add_parser("compare")
    compare.add_argument(
        "--bench-root", default="bench", help="Benchmark root directory"
    )
    compare.add_argument(
        "--build",
        action="append",
        required=True,
        help="Path to a build spec JSON. Repeat in display order.",
    )
    compare.add_argument(
        "--benchmark",
        action="append",
        help="Benchmark id to run. Repeat to select multiple.",
    )
    compare.add_argument(
        "--markdown-output", help="Write grouped markdown to this file"
    )
    compare.set_defaults(func=cmd_compare)

    args = parser.parse_args()
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
