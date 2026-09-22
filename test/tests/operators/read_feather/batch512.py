# runner: python

import json

from feather_test_utils import encode_zeek, run

encoded = encode_zeek(512)
decoded = run(
    "load_stdin\nread_feather\nmeasure\ndrop timestamp\nwrite_json compact=true",
    data=encoded,
)
assert not decoded.stderr, decoded.stderr.decode()
rows = [json.loads(line) for line in decoded.stdout.splitlines()]
expected_sizes = [512] * 16 + [270]
assert rows == [{"events": size, "selected": size} for size in expected_sizes], rows
for row in rows:
    print(f"{{\n  events: {row['events']},\n  selected: {row['selected']},\n}}")
