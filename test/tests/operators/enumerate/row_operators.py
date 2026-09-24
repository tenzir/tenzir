# runner: python
# timeout: 120

"""Exercise Nova row operators across masked, heterogeneous batch boundaries."""

from __future__ import annotations

import json
import os
import shlex
import subprocess

BINARY = [*shlex.split(os.environ.get("TENZIR_BINARY", "tenzir")), "--nova=true"]


def run(pipeline: str, rows: list[dict] | None = None, batch_size: int = 3):
    if rows is not None:
        pipeline = f"load_stdin | read_json _batch_size={batch_size} | {pipeline}"
    result = subprocess.run(
        [*BINARY, "--bare-mode", pipeline + " | write_json compact=true"],
        input="" if rows is None else "\n".join(map(json.dumps, rows)),
        text=True,
        capture_output=True,
        timeout=15,
    )
    assert result.returncode == 0, (pipeline, result.stderr)
    assert not result.stderr, (pipeline, result.stderr)
    return [json.loads(line) for line in result.stdout.splitlines()]


def main():
    rows = [{"id": i, "keep": i % 3 != 1} for i in range(11)]
    active = [row for row in rows if row["keep"]]
    for size in (1, 2, 5, 20):
        for count in (0, 1, 3):
            assert run(f"where keep | repeat {count}", rows, size) == active * count
        numbered = run("where keep | enumerate", rows, size)
        assert numbered == [{"#": i, **row} for i, row in enumerate(active)]
        assert all(next(iter(row)) == "#" for row in numbered)
    for op in ("repeat", "enumerate"):
        assert run(op, []) == []
        assert run("where false | " + op, rows) == []
    nested = [
        {"v": {"a": 1}, "list": [1, "x", None]},
        {"v": "text", "list": []},
        {"other": True, "v": {"b": [2, {"c": False}]}},
        {"v": None, "list": [{"x": 1}, {"y": 2}]},
    ]
    for size in (2, 20):
        assert run("repeat 2", nested, size) == nested * 2
    assert run("from {x: 1}, {x: 1.uint()}, {x: 2}, {x: 1} | enumerate n, group=x") == [
        {"n": 0, "x": 1},
        {"n": 1, "x": 1},
        {"n": 0, "x": 2},
        {"n": 2, "x": 1},
    ]
    groups = [
        {"g": x} for x in (None, "a", {"x": 1}, [1, 2], None, "a", {"x": 1}, [1, 2])
    ]
    assert run("enumerate n, group=g", groups, 3) == [
        {"n": i // 4, **row} for i, row in enumerate(groups)
    ]
    numbered = run(
        "enumerate p.n",
        [{"p": {"x": 1}}, {}, {"p": None}, {"p": {"n": 9, "x": 2}}],
    )
    assert numbered == [
        {"p": {"n": 0, "x": 1}},
        {"p": {"n": 1}},
        {"p": {"n": 2}},
        {"p": {"n": 3, "x": 2}},
    ]
    assert all(next(iter(row["p"])) == "n" for row in numbered)
    overwritten = run("enumerate x", [{"a": True, "x": 9}, {"a": False}])
    assert list(overwritten[0]) == ["a", "x"]
    assert list(overwritten[1]) == ["x", "a"]
    invalid = subprocess.run(
        [*BINARY, "--bare-mode", "from {x: 1} | enumerate this"],
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert invalid.returncode != 0
    assert "enumerate output must be a field path" in invalid.stderr
    assert "internal error" not in invalid.stderr
    print("Nova row operator regressions passed")


if __name__ == "__main__":
    main()
