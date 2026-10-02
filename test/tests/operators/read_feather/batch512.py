# runner: python

import json

from feather_test_utils import encode_zeek, run

encoded = encode_zeek(512)
decoded = run(
    "load_stdin\nread_feather\nmeasure by_schema=false\ndrop timestamp\n"
    "write_json compact=true",
    data=encoded,
)
assert not decoded.stderr, decoded.stderr.decode()
rows = [json.loads(line) for line in decoded.stdout.splitlines()]
# `measure by_schema=false` reports one metric per batch, regardless of how many
# schemas its events have.
expected_sizes = [512] * 16 + [270]
sizes = [row["events"] for row in rows]
assert sizes == expected_sizes, (sizes, rows)
for size in sizes:
    print(f"{{\n  events: {size},\n}}")
