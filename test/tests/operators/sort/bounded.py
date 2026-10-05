# runner: python
# timeout: 600

"""Compare bounded sorting with an optimization-barrier full-sort reference.

Each comparison starts two engine processes; allow their cumulative startup
cost on busy static-build runners without relaxing the per-process timeout.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import json
import os
import shlex
import subprocess

BINARY = [*shlex.split(os.environ["TENZIR_BINARY"]), "--nova=true", "--bare-mode"]


def run(pipeline: str, rows: list[dict], batch_size: int = 3) -> list[dict]:
    result = subprocess.run(
        [
            *BINARY,
            f"load_stdin | read_json _batch_size={batch_size} | {pipeline}"
            " | write_json compact=true",
        ],
        input="\n".join(map(json.dumps, rows)),
        text=True,
        capture_output=True,
        timeout=15,
    )
    assert result.returncode == 0, (pipeline, result.stderr)
    assert not result.stderr, (pipeline, result.stderr)
    return [json.loads(line) for line in result.stdout.splitlines()]


def compare(sort: str, suffix: str, rows: list[dict], size: int = 3) -> list[dict]:
    bounded = run(f"{sort} | {suffix}", rows, size)
    full = run(f"{sort} | optimize_barrier | {suffix}", rows, size)
    assert bounded == full, (sort, suffix, bounded, full)
    return bounded


def check_batch_size(rows: list[dict], size: int) -> None:
    full = run("sort key? | optimize_barrier", rows, size)
    for count in (0, 1, 4, 100):
        assert run(f"sort key? | head {count}", rows, size) == full[:count]
    for sort in ("sort -key?", "sort key?, -secondary", "sort"):
        compare(sort, "head 4", rows, size)


def check_residual_filter() -> None:
    # The predicate stays residual at the source, but must run before top-N.
    assert compare(
        "sort key",
        "where keep | head 2",
        [
            {"key": 0, "keep": False},
            {"key": 1, "keep": False},
            {"key": 2, "keep": True},
            {"key": 3, "keep": True},
        ],
    ) == [{"key": 2, "keep": True}, {"key": 3, "keep": True}]


def check_aggregation(counts: list[dict], operator: str, first: dict) -> None:
    # Aggregation counts the complete input; only its final sort is bounded.
    full = run(f"{operator} | optimize_barrier", counts)
    assert full[0] == first
    for count in (0, 1, 3, 100):
        assert run(f"{operator} | head {count}", counts) == full[:count]
    compare(operator, "where count >= 2 | head 2", counts)


rows = [
    {"id": i, "key": key, "secondary": i % 3, "keep": i % 4 != 0}
    for i, key in enumerate([None, 1, "b", 0, 1.0, -2, "a", None, 1, 2, -1])
]
rows[2]["extra"] = {"nested": [1, None, "x"]}
rows[7].pop("key")
nested = [
    {"id": i, "key": key}
    for i, key in enumerate(
        [
            [1, 3],
            {"b": 0},
            [1, 2],
            {"a": 1},
            None,
            [1, 2],
        ]
    )
]
counts = [{"x": x} for x in ("b", "a", "a", "b", "a", "d", "c", "c")]

# These independent cases launch 67 CLI processes. Bound concurrency to avoid
# paying every startup cost serially within the test's overall timeout.
with ThreadPoolExecutor(max_workers=4) as executor:
    checks = [executor.submit(check_batch_size, rows, size) for size in (1, 3, 20)]
    for suffix in (
        "where keep | where id > 4 | head 3 | select id",
        "head 4 | where keep | head 2",
        "select id | head 3",
        "where keep | select id | head 3",
        "head 7 | head 2",
        "where false | head 2",
    ):
        checks.append(executor.submit(compare, "sort key?", suffix, rows))
    checks.append(executor.submit(compare, "sort key?", "head 2", []))
    checks.append(executor.submit(check_residual_filter))
    for sort in ("sort key", "sort -key"):
        checks.append(executor.submit(compare, sort, "head 4", nested))
    for operator, first in (
        ("top x", {"x": "a", "count": 3}),
        ("rare x", {"x": "d", "count": 1}),
    ):
        checks.append(executor.submit(check_aggregation, counts, operator, first))
    for check in checks:
        check.result()

print("Bounded sort, projection, filter, top, and rare regressions passed")
