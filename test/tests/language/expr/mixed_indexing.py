# runner: python

"""Exercise mixed index types and per-row record order under sparse masks."""

import json
import os
import shlex
import subprocess

BINARY = [*shlex.split(os.environ["TENZIR_BINARY"]), "--bare-mode", "--nova=true"]


def run(rows, expression, batch_size=64):
    result = subprocess.run(
        [
            *BINARY,
            f"""from_stdin {{ read_tql _batch_size={batch_size} }}
where keep
select id, result={expression}
write_ndjson""",
        ],
        input="\n".join(map(json.dumps, rows)),
        text=True,
        capture_output=True,
        timeout=15,
    )
    assert result.returncode == 0, result.stderr
    assert not result.stderr, result.stderr
    return [json.loads(line) for line in result.stdout.splitlines()]


rows = [
    {"id": 0, "subject": {"a": 10, "b": 11}, "key": "b", "keep": True},
    {"id": 1, "subject": [20, 21], "key": -1, "keep": True},
    {"id": 2, "subject": {"b": 30, "a": 31}, "key": 0, "keep": True},
    {"id": 3, "subject": {"a": 40}, "key": 2, "keep": True},
    {"id": 4, "subject": None, "key": "a", "keep": True},
    {"id": 5, "subject": {"a": 50}, "key": None, "keep": True},
    {"id": 6, "subject": [60], "key": 1, "keep": True},
    {"id": 7, "subject": {"a.b": 70, "a": {"b": 71}}, "key": "a.b", "keep": True},
    {"id": 8, "subject": [80], "key": 18446744073709551615, "keep": True},
    {"id": 9, "subject": True, "key": False, "keep": False},
]
expected = [
    {"id": i, "result": value}
    for i, value in enumerate([11, 21, 30, None, None, None, None, 70, None])
]
for batch_size in (1, 3, 64):
    assert run(rows, "subject[key]?", batch_size) == expected

records = [
    {"id": 0, "subject": {"a": 10, "b": 11}, "key": "b", "keep": True},
    {"id": 1, "subject": {"b": 20, "a": 21}, "key": "a", "keep": True},
    {"id": 2, "subject": None, "key": "a", "keep": True},
    {"id": 3, "subject": {"a": 30}, "key": None, "keep": True},
    {"id": 4, "subject": [40], "key": 0, "keep": False},
]
for expression, values in (
    ('subject["a"]?', [10, 21, None, 30]),
    ("subject[key]?", [11, 21, None, None]),
    ("subject[0]?", [10, 20, None, 30]),
    ("subject[1]?", [11, 21, None, None]),
):
    assert run(records, expression) == [
        {"id": i, "result": value} for i, value in enumerate(values)
    ]

lists = [
    {"id": 0, "subject": [10, 11], "keep": True},
    {"id": 1, "subject": [], "keep": True},
    {"id": 2, "subject": None, "keep": True},
    {"id": 3, "subject": {"a": 30}, "keep": False},
]
for expression, values in (
    ("subject[0]?", [10, None, None]),
    ("subject[-1]?", [11, None, None]),
    ("subject[18446744073709551615]?", [None, None, None]),
):
    assert run(lists, expression) == [
        {"id": i, "result": value} for i, value in enumerate(values)
    ]
assert (
    run([{"id": 0, "subject": True, "key": False, "keep": False}], "subject[key]") == []
)
print("Mixed indexing preserves row types, masks, and record order")
