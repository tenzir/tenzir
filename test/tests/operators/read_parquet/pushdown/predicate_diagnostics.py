# runner: python
# timeout: 120

"""Check diagnostic parity between pushed and standalone Nova predicates."""

import json
import os
from pathlib import Path
import shlex
import subprocess


def run(predicates: str, *, pushed: bool) -> subprocess.CompletedProcess[str]:
    path = Path(os.environ["READ_PUSHDOWN_ROOT"]) / "types"
    # The fixture has four rows. This head preserves the input but blocks filter
    # pushdown. Padding keeps predicate source locations identical in both plans.
    barrier = " " * len("head 4") if pushed else "head 4"
    pipeline = (
        f"from_file {json.dumps(str(path))} {{ read_parquet }}\n"
        f"{barrier}\n{predicates}\nselect id\nwrite_json compact=true"
    )
    result = subprocess.run(
        [*shlex.split(os.environ["TENZIR_BINARY"]), "--bare-mode", "--nova", pipeline],
        capture_output=True,
        text=True,
        timeout=20,
    )
    assert result.returncode == 0, (pipeline, result.stderr)
    return result


def main():
    cases = [
        ("where null", [], ["warning: expected `bool`"]),
        (
            "where absent",
            [],
            ["warning: event does not have field", "warning: expected `bool`"],
        ),
        ("where flag", [0, 3], ["warning: expected `bool`"]),
        ("where id != 1\nwhere flag", [0, 3], []),
        ("where false\nwhere absent", [], []),
        ("where id >= 2", [2, 3], []),
        ("where id", [], ["warning: expected `bool`"]),
    ]
    for predicates, ids, warnings in cases:
        pushed = run(predicates, pushed=True)
        standalone = run(predicates, pushed=False)
        assert pushed.stdout == standalone.stdout, (
            predicates,
            pushed.stdout,
            standalone.stdout,
        )
        assert pushed.stderr == standalone.stderr, (
            predicates,
            pushed.stderr,
            standalone.stderr,
        )
        rows = [json.loads(line) for line in pushed.stdout.splitlines()]
        assert rows == [{"id": value} for value in ids], (predicates, rows)
        actual = [
            line for line in pushed.stderr.splitlines() if line.startswith("warning:")
        ]
        assert actual == warnings, (predicates, actual)
    print("Pushed and standalone Nova predicates preserve rows and diagnostics")


if __name__ == "__main__":
    main()
