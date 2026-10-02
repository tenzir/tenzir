# runner: python

"""`type_id(this)` returns the `schema_id` that `measure` reports.

The explorer in the Tenzir Platform lists the schemas of a result with
`measure` and then fetches the events of one schema with
`where type_id(this) == "<schema_id>"`, so both must agree. Schemas are
identified by their structure alone, so events that share a structure but not
their name share an identifier.
"""

import json
import os
import shlex
import subprocess


def run(pipeline: str) -> list[dict]:
    command = [
        *shlex.split(os.environ["TENZIR_BINARY"]),
        "--bare-mode",
        "--nova=true",
        f"{pipeline}\nwrite_json compact=true",
    ]
    result = subprocess.run(command, capture_output=True, timeout=20)
    assert result.returncode == 0, (pipeline, result.stderr.decode())
    return [json.loads(line) for line in result.stdout.splitlines()]


events = 'from {a: 1, b: "x"}, {c: 3}, {a: 2, b: "y"}\n'
ids = [row["id"] for row in run(events + "select id = type_id(this)")]
measured = [row["schema_id"] for row in run(events + "measure\nselect schema_id")]
assert len(set(ids)) == 2, ids
assert set(ids) == set(measured), (ids, measured)
for id, expected in zip(ids, [2, 1, 2]):
    selected = run(events + f"where type_id(this) == {json.dumps(id)}")
    assert len(selected) == expected, (id, selected)
# The name is not part of the identifier.
renamed = run(
    'from {a: 1, b: "x"}\n@name = "foo"\nselect id = type_id(this)',
)
assert renamed[0]["id"] == ids[0], (renamed, ids)
print(f"schemas: {len(set(ids))}")
