#!/usr/bin/env python3

import argparse
import json
import re
import shlex
import subprocess
import sys
from collections import defaultdict, deque
from pathlib import Path

SOURCE_SUFFIXES = {".c", ".cc", ".cpp", ".cxx"}
HEADER_SUFFIXES = {".h", ".hpp"}
ALL_SUFFIXES = SOURCE_SUFFIXES | HEADER_SUFFIXES
SOURCE_ROOTS = ("libtenzir", "libtenzir_test", "plugins", "tenzir")
INCLUDE_PATTERN = re.compile(r'^\s*#\s*include\s*[<"]([^">]+)[">]', re.MULTILINE)
HUNK_PATTERN = re.compile(r"^@@ .*\+(\d+)(?:,(\d+))? @@")


def git(root: Path, *args: str) -> str:
    return subprocess.check_output(
        ["git", "-C", root, *args], text=True, stderr=subprocess.DEVNULL
    )


def changed_lines(root: Path, base: str) -> dict[Path, list[list[int]]]:
    result: dict[Path, list[list[int]]] = defaultdict(list)
    path: Path | None = None
    diff = git(
        root,
        "diff",
        "--no-ext-diff",
        "--unified=0",
        "--diff-filter=ACMR",
        base,
        "HEAD",
        "--",
        *(f"engine/{source_root}" for source_root in SOURCE_ROOTS),
    )
    for line in diff.splitlines():
        if line.startswith("+++ b/"):
            candidate = (root / line[6:]).resolve()
            try:
                relative = candidate.relative_to(root / "engine")
            except ValueError:
                path = None
                continue
            path = (
                candidate
                if relative.parts[0] in SOURCE_ROOTS
                and candidate.suffix in ALL_SUFFIXES
                and "aux" not in relative.parts
                else None
            )
            continue
        if path is None:
            continue
        match = HUNK_PATTERN.match(line)
        if not match:
            continue
        start = int(match.group(1))
        count = int(match.group(2) or 1)
        if count:
            result[path].append([start, start + count - 1])
    return result


def compilation_database(path: Path) -> tuple[set[Path], set[Path]]:
    entries = json.loads(path.read_text())
    sources = set()
    include_directories = set()
    for entry in entries:
        directory = Path(entry["directory"])
        source = Path(entry["file"])
        sources.add(
            (directory / source).resolve()
            if not source.is_absolute()
            else source.resolve()
        )
        arguments = entry.get("arguments")
        if arguments is None:
            arguments = shlex.split(entry["command"])
        index = 0
        while index < len(arguments):
            argument = arguments[index]
            include = None
            if argument in {"-I", "-isystem", "-iquote"} and index + 1 < len(arguments):
                index += 1
                include = arguments[index]
            elif argument.startswith("-I") and len(argument) > 2:
                include = argument[2:]
            if include:
                include_path = Path(include)
                include_directories.add(
                    (directory / include_path).resolve()
                    if not include_path.is_absolute()
                    else include_path.resolve()
                )
            index += 1
    return sources, include_directories


def first_party_files(engine: Path) -> set[Path]:
    result = set()
    for source_root in SOURCE_ROOTS:
        for path in (engine / source_root).rglob("*"):
            if path.suffix not in ALL_SUFFIXES:
                continue
            relative = path.relative_to(engine)
            if "aux" not in relative.parts and "build" not in relative.parts:
                result.add(path.resolve())
    return result


def shared_prefix_length(lhs: Path, rhs: Path) -> int:
    for index, (left, right) in enumerate(zip(lhs.parts, rhs.parts)):
        if left != right:
            return index
    return min(len(lhs.parts), len(rhs.parts))


def include_graph(
    files: set[Path], include_directories: set[Path]
) -> dict[Path, set[Path]]:
    reverse: dict[Path, set[Path]] = defaultdict(set)
    by_spelling: dict[str, set[Path]] = defaultdict(set)
    for path in files:
        by_spelling[path.name].add(path)
        parts = path.parts
        for index, part in enumerate(parts):
            if part == "include":
                by_spelling["/".join(parts[index + 1 :])].add(path)
    for including in files:
        try:
            contents = including.read_text(errors="ignore")
        except OSError:
            continue
        for spelling in INCLUDE_PATTERN.findall(contents):
            local = (including.parent / spelling).resolve()
            if local in files:
                candidates = {local}
            else:
                candidates = set(by_spelling.get(spelling, ()))
                if not candidates:
                    candidates = {
                        candidate
                        for directory in include_directories
                        if (candidate := (directory / spelling).resolve()) in files
                    }
                if len(candidates) > 1:
                    best_score = max(
                        shared_prefix_length(including, candidate)
                        for candidate in candidates
                    )
                    candidates = {
                        candidate
                        for candidate in candidates
                        if shared_prefix_length(including, candidate) == best_score
                    }
            for included in candidates:
                reverse[included].add(including)
    return reverse


def translation_unit(
    header: Path, reverse: dict[Path, set[Path]], sources: set[Path]
) -> Path | None:
    queue = deque([header])
    visited = {header}
    while queue:
        included = queue.popleft()
        for including in sorted(reverse.get(included, ()), key=str):
            if including in visited:
                continue
            if including in sources:
                return including
            visited.add(including)
            queue.append(including)
    return None


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Select compilation units and changed lines for clang-tidy"
    )
    parser.add_argument("base", help="Git revision to compare against HEAD")
    parser.add_argument("database", type=Path, help="compile_commands.json path")
    args = parser.parse_args()

    root = Path(git(Path.cwd(), "rev-parse", "--show-toplevel").strip())
    engine = root / "engine"
    lines = changed_lines(root, args.base)
    sources, include_directories = compilation_database(args.database.resolve())
    selected = {
        path for path in lines if path.suffix in SOURCE_SUFFIXES and path in sources
    }

    headers = [path for path in lines if path.suffix in HEADER_SUFFIXES]
    if headers:
        reverse = include_graph(first_party_files(engine), include_directories)
        unmapped = []
        for header in headers:
            source = translation_unit(header, reverse, sources)
            if source is None:
                unmapped.append(header)
            else:
                selected.add(source)
        if unmapped:
            print(
                "warning: no compilation unit includes these changed headers:\n  "
                + "\n  ".join(str(path.relative_to(root)) for path in unmapped),
                file=sys.stderr,
            )

    plan = {
        "line_filter": [
            {"name": str(path), "lines": ranges}
            for path, ranges in sorted(lines.items(), key=lambda item: str(item[0]))
        ],
        "sources": [str(path) for path in sorted(selected, key=str)],
    }
    json.dump(plan, sys.stdout, separators=(",", ":"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
