# runner: python

from collections import Counter
import json

from feather_test_utils import encode_zeek, run

encoded = encode_zeek(256, compressed=True)
decoded = run(
    "load_stdin\nsplit_bytes 1\nread_feather\nselect service\nwrite_json compact=true",
    data=encoded,
)
assert not decoded.stderr, decoded.stderr.decode()
counts = Counter(json.loads(line)["service"] for line in decoded.stdout.splitlines())
expected = {
    "dns": 4067,
    "ftp": 2,
    "ftp-data": 2,
    "http": 2386,
    "smtp": 21,
    "ssl": 121,
    None: 1863,
}
assert counts == expected, counts
for service in expected:
    print(f"{{\n  total: {counts[service]},\n  service: {json.dumps(service)},\n}}")
