#!/usr/bin/env python3
"""Parse `/bench` pull request comments into machine-readable JSON."""

from __future__ import annotations

import argparse
import json
import shlex
from fnmatch import fnmatch
from pathlib import Path

from common import bench_root


def available_benchmarks(root: Path) -> list[str]:
    benchmarks_root = root / "benchmarks"
    if not benchmarks_root.exists():
        return []
    return sorted(
        directory.name
        for directory in benchmarks_root.iterdir()
        if directory.is_dir() and (directory / "bench.yaml").exists()
    )


def load_defaults(root: Path) -> list[str]:
    defaults_file = root / "defaults.txt"
    if not defaults_file.exists():
        return []
    values: list[str] = []
    for raw_line in defaults_file.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        values.append(line)
    return values


def parse_command(comment_body: str, *, root: Path | None = None) -> dict[str, object]:
    body = comment_body.strip()
    if not body.startswith("/bench"):
        raise ValueError("comment does not start with /bench")
    root = (root or bench_root()).resolve()
    tail = body[len("/bench") :].strip()
    args: dict[str, str] = {}
    if tail:
        for token in shlex.split(tail):
            if "=" not in token:
                raise ValueError(f"invalid token: {token}")
            key, value = token.split("=", 1)
            if key not in {"benchmarks", "targets", "refs"}:
                raise ValueError(f"unsupported argument: {key}")
            if key in args:
                raise ValueError(f"duplicate argument: {key}")
            args[key] = value
    benchmarks = resolve_benchmarks(
        args.get("benchmarks"),
        root=root,
    )
    refs = resolve_refs(args.get("refs"))
    targets = resolve_targets(args.get("targets"), refs=refs)
    return {
        "benchmarks": benchmarks,
        "targets": targets,
        "refs": refs,
        "raw": comment_body,
    }


def resolve_benchmarks(value: str | None, *, root: Path) -> list[str]:
    patterns = [
        item.strip()
        for item in (value.split(",") if value else load_defaults(root))
        if item.strip()
    ]
    available = available_benchmarks(root)
    selected: list[str] = []
    seen: set[str] = set()
    for pattern in patterns:
        matches = [benchmark for benchmark in available if fnmatch(benchmark, pattern)]
        if not matches:
            raise ValueError(f"benchmark pattern matched nothing: {pattern}")
        for benchmark in matches:
            if benchmark in seen:
                continue
            seen.add(benchmark)
            selected.append(benchmark)
    return selected


def resolve_targets(
    value: str | None, *, refs: list[dict[str, str]] | None = None
) -> list[str]:
    targets = (
        [item.strip() for item in value.split(",") if item.strip()]
        if value is not None
        else ["docker"]
    )
    for selector in refs or []:
        target = selector["target"]
        if target not in targets:
            targets.append(target)
    for target in targets:
        if target not in {"docker", "static"}:
            raise ValueError(f"unsupported target: {target}")
    deduped: list[str] = []
    seen: set[str] = set()
    for target in targets:
        if target in seen:
            continue
        seen.add(target)
        deduped.append(target)
    return deduped


def resolve_refs(value: str | None) -> list[dict[str, str]]:
    if value is None:
        return []
    refs: list[dict[str, str]] = []
    for raw_entry in value.split(","):
        entry = raw_entry.strip()
        if not entry:
            continue
        if "@" not in entry:
            raise ValueError(f"invalid ref selector: {entry}")
        target, ref = entry.split("@", 1)
        if target not in {"docker", "static"}:
            raise ValueError(f"unsupported ref target: {target}")
        if not ref:
            raise ValueError(f"missing ref value for target: {target}")
        refs.append({"target": target, "ref": ref})
    return refs


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--comment-body", required=True, help="Comment body to parse")
    parser.add_argument(
        "--bench-root",
        type=Path,
        default=bench_root(),
        help="Path to the benchmark root",
    )
    args = parser.parse_args()
    payload = parse_command(args.comment_body, root=args.bench_root)
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
