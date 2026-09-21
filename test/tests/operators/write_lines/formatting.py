# timeout: 120

import os
import shlex
import subprocess


BINARY = [*shlex.split(os.environ["TENZIR_BINARY"]), "--nova=true"]


def write(source, jobs=0):
    option = f" _jobs={jobs}" if jobs else ""
    result = subprocess.run(
        [*BINARY, "--bare-mode", f"{source} | write_lines{option}"],
        capture_output=True,
        timeout=10,
    )
    assert result.returncode == 0, (source, result.stderr)
    assert not result.stderr, (source, result.stderr)
    return result.stdout


cases = [
    ("from {}", b"\n"),
    ("from {a: null, b: null}", b"\n"),
    ('from {a: "", b: "two words", c: "x\\ny"}', b" two words x\ny\n"),
    ("from {a: 42, b: true, c: -2, d: 1.5}", b"42 true -2 1.5\n"),
    (
        'from {a: b"hello", b: 192.0.2.1, c: 192.0.2.0/24}',
        b"aGVsbG8= 192.0.2.1 192.0.2.0/24\n",
    ),
    ('from {a: "left", b: null, c: "right"}', b"left right\n"),
    ("from {a: {b: 1, c: {d: 2}}, e: {}, f: 3}", b"1 2 3\n"),
    ('from {a: [], b: [null, null], c: "end"}', b"end\n"),
    ('from {a: [1, null, 2], b: ["", "x", ""]}', b"1,2 ,x,\n"),
    ("from {a: [[1, 2], [], [3, null]]}", b"1,2,3\n"),
    ("from {a: [{x: 1, y: 2}, {x: 3, y: 4}]}", b"1 2,3 4\n"),
    ("from {a: [{y: 2, x: 1}, {x: 3, y: 4}]}", b"2 1,3 4\n"),
    ("from {a: [{x: null, y: 2}, {x: 3, y: null}]}", b"2,3\n"),
    ("from {a: [{x: null, y: 2}, {x: null, y: 4}]}", b"2,4\n"),
    ("from {a: [{x: [1, 2], y: 3}, {x: [], y: 4}]}", b"1,2 3,4\n"),
    ("from {a: [[], [{x: 1, y: 2}]]}", b"1 2\n"),
    ("from {a: [[], [{}]], b: 1}", b"1\n"),
    ("from {a: [{x: null, y: 2}, {x: {z: 3}, y: 4}]}", b"2,3 4\n"),
    ('from {"a.b": 1, a: {b: 2}}', b"1 2\n"),
    ("from {b: 2, a: 1}, {a: 3, b: 4}", b"2 1\n3 4\n"),
    ("from {x: 1}, {x: 2}, {x: 3} | where x != 2", b"1\n3\n"),
    ("from {x: 1}, {x: 2} | where x > 2", b""),
]
for jobs in (0, 1, 3):
    for source, expected in cases:
        actual = write(source, jobs)
        if jobs:
            assert sorted(actual.splitlines()) == sorted(expected.splitlines()), (
                jobs,
                source,
                actual,
                expected,
            )
        else:
            assert actual == expected, (source, actual, expected)

    # More than one batch, with unique rows, detects lost or duplicated work.
    actual = write(
        "from {x: 0} | repeat 20000 | enumerate row | where row >= 10000 | drop x",
        jobs,
    )
    expected = [str(i).encode() for i in range(10000, 20000)]
    assert sorted(actual.splitlines()) == sorted(expected), jobs

print("formatting, masks, and workers: ok")
