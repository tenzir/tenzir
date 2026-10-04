# runner: python
# timeout: 300

"""Compare bounded sorting with an optimization-barrier full-sort reference."""

from __future__ import annotations

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


rows = [
    {"id": i, "key": key, "secondary": i % 3, "keep": i % 4 != 0}
    for i, key in enumerate([None, 1, "b", 0, 1.0, -2, "a", None, 1, 2, -1])
]
rows[2]["extra"] = {"nested": [1, None, "x"]}
rows[7].pop("key")
for size in (1, 3, 20):
    full = run("sort key? | optimize_barrier", rows, size)
    for count in (0, 1, 4, 100):
        assert run(f"sort key? | head {count}", rows, size) == full[:count]
    for sort in ("sort -key?", "sort key?, -secondary", "sort"):
        compare(sort, "head 4", rows, size)
for suffix in (
    "where keep | where id > 4 | head 3 | select id",
    "head 4 | where keep | head 2",
    "select id | head 3",
    "where keep | select id | head 3",
    "head 7 | head 2",
    "where false | head 2",
):
    compare("sort key?", suffix, rows)
compare("sort key?", "head 2", [])

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
for sort in ("sort key", "sort -key"):
    compare(sort, "head 4", nested)

# Aggregation still counts the complete input; only its final sort is bounded.
counts = [{"x": x} for x in ("b", "a", "a", "b", "a", "d", "c", "c")]
for operator, first in (
    ("top x", {"x": "a", "count": 3}),
    ("rare x", {"x": "d", "count": 1}),
):
    full = run(f"{operator} | optimize_barrier", counts)
    assert full[0] == first
    for count in (0, 1, 3, 100):
        assert run(f"{operator} | head {count}", counts) == full[:count]
    compare(operator, "where count >= 2 | head 2", counts)

print("Bounded sort, projection, filter, top, and rare regressions passed")
