import json
import os
import shlex
import subprocess
import re
import uuid


def run(pipeline):
    result = subprocess.run(
        [
            *shlex.split(os.environ["TENZIR_BINARY"]),
            "--bare-mode",
            "--nova=true",
            pipeline,
        ],
        capture_output=True,
        timeout=15,
    )
    return result


def rows(pipeline):
    result = run(pipeline + " | write_ndjson")
    assert result.returncode == 0, result.stderr
    assert not result.stderr, result.stderr
    return [json.loads(line) for line in result.stdout.splitlines()]


for version in ("nil", "v1", "v4", "v6", "v7"):
    output = rows(f'from {{x: 1}} | repeat 25 | value=uuid(version="{version}")')
    values = [row["value"] for row in output]
    parsed = [uuid.UUID(value) for value in values]
    if version == "nil":
        assert all(value.int == 0 for value in parsed)
    else:
        assert len(set(values)) == 25, (version, values)
        assert all(value.version == int(version[1]) for value in parsed)
        pattern = rf"[0-9a-f]{{8}}-[0-9a-f]{{4}}-{version[1]}[0-9a-f]{{3}}-[89ab][0-9a-f]{{3}}-[0-9a-f]{{12}}"
        assert all(re.fullmatch(pattern, value) for value in values), values
assert uuid.UUID(rows("from {} | value=uuid()")[0]["value"]).version == 4
result = run('from {} | x=uuid(version="bad") | write_ndjson')
assert result.returncode != 0
assert b"unsupported UUID version" in result.stderr, result.stderr

print("uuid: ok")
