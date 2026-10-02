# runner: python

import json

from feather_test_utils import encode_zeek, run

encoded = encode_zeek(512)
decoded = run(
    "load_stdin\nread_feather\nmeasure\ndrop timestamp, schema_id, schema\n"
    "write_json compact=true",
    data=encoded,
)
assert not decoded.stderr, decoded.stderr.decode()
rows = [json.loads(line) for line in decoded.stdout.splitlines()]
# `measure` reports one metric per schema, and rows that leave a field null
# have a schema of their own, so a batch shows up as a run of metrics that add
# up to its size.
expected_sizes = [512] * 16 + [270]
sizes = []
pending = 0
for row in rows:
    pending += row["events"]
    if pending >= expected_sizes[len(sizes)]:
        sizes.append(pending)
        pending = 0
assert pending == 0, rows
assert sizes == expected_sizes, (sizes, rows)
for size in sizes:
    print(f"{{\n  events: {size},\n}}")
