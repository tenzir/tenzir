# runner: python

import json
import os
import shlex
import subprocess


binary = [*shlex.split(os.environ["TENZIR_BINARY"]), "--bare-mode", "--nova=true"]
source = (
    b"#separator \\x09\r\n"
    b"#path\tframing\r\n"
    b"#fields\tmessage\tvalue\r\n"
    b"#types\tstring\tcount\r\n"
    b"first\t1\r\n\r\n"
    b"second\t2\r"
    b"third\t3\n"
    b"last\t4"
)
expected = [
    {"message": "first", "value": 1, "schema": "zeek.framing"},
    {"message": "second", "value": 2, "schema": "zeek.framing"},
    {"message": "third", "value": 3, "schema": "zeek.framing"},
    {"message": "last", "value": 4, "schema": "zeek.framing"},
]
for reader in ("read_zeek_tsv", "read_auto"):
    for size in (1, 2, 7, 64):
        result = subprocess.run(
            [
                *binary,
                f"load_stdin | split_bytes {size} | {reader} "
                "| schema = @name | write_ndjson",
            ],
            input=source,
            capture_output=True,
            timeout=20,
        )
        assert result.returncode == 0 and not result.stderr, result.stderr
        actual = [json.loads(line) for line in result.stdout.splitlines()]
        assert actual == expected, (reader, size, actual)

# A header without a body and completely empty input must not produce events.
for source in (b"", b"#path\tempty\n#fields\tx\n#types\tstring\n"):
    result = subprocess.run(
        [*binary, "load_stdin | read_zeek_tsv | write_ndjson"],
        input=source,
        capture_output=True,
        timeout=20,
    )
    assert result.returncode == 0 and not result.stderr, result.stderr
    assert not result.stdout, result.stdout
