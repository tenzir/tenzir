# runner: python
# timeout: 120

import json
import os
from pathlib import Path
import shlex
import subprocess


binary = [*shlex.split(os.environ["TENZIR_BINARY"]), "--bare-mode", "--nova=true"]
schemas = Path(__file__).parent / "schemas"
source = b"kind: first\nvalue: 42\ntext: 123\nsrc: 192.0.2.1\nextra: yes\n"
for policy in ('schema="yaml.first"', 'selector="kind:yaml"'):
    for schema_only in (False, True):
        for raw in (False, True):
            result = subprocess.run(
                [
                    *binary,
                    f"--schema-dirs={schemas}",
                    f"load_stdin | read_yaml {policy}, "
                    f"schema_only={str(schema_only).lower()}, raw={str(raw).lower()} "
                    "| select event=this, schema=@name, src_type=type_of(src).kind "
                    "| write_ndjson",
                ],
                input=source,
                capture_output=True,
                timeout=20,
            )
            assert result.returncode == 0 and not result.stderr, result.stderr
            actual = json.loads(result.stdout)
            assert actual == {
                "event": {
                    "kind": "first",
                    "value": 42,
                    "text": "123",
                    "src": "192.0.2.1",
                    **({} if schema_only else {"extra": True}),
                },
                "schema": "yaml.first",
                "src_type": "ip",
            }, (policy, schema_only, raw, actual)

# Both schema and selector policies follow the standard builder behavior:
# the last value of a repeated key wins, independently of schema_only.
source = b"""kind: duplicates
a: {x: 1}
a: {y: 2}
l: [1]
l: [2, 3]
text: 123
src: 192.0.2.1
extra: {b: 2}
extra: null
extra: null
scalar: 1
scalar.b: 2
reverse.b: 2
reverse: 1
"""
for policy in ('schema="yaml.duplicates"', 'selector="kind:yaml"'):
    for schema_only in (False, True):
        for raw in (False, True):
            for batch_size in (1, 7):
                event = {
                    "kind": "duplicates",
                    "a": {"x": None, "y": 2},
                    "l": [2, 3],
                    "text": "123",
                    "src": "192.0.2.1",
                }
                if not schema_only:
                    event.update(
                        extra=None,
                        scalar={"b": 2},
                        reverse=1,
                    )
                result = subprocess.run(
                    [
                        *binary,
                        f"--schema-dirs={schemas}",
                        f"load_stdin | split_bytes 7 | read_yaml {policy}, "
                        f'unflatten_separator=".", _batch_size={batch_size}, '
                        f"schema_only={str(schema_only).lower()}, raw={str(raw).lower()} "
                        "| select event=this, schema=@name, src_type=type_of(src).kind "
                        "| write_ndjson",
                    ],
                    input=b"...\n".join([source] * 3),
                    capture_output=True,
                    timeout=20,
                )
                assert result.returncode == 0 and not result.stderr, result.stderr
                actual = [json.loads(line) for line in result.stdout.splitlines()]
                assert (
                    actual
                    == [{"event": event, "schema": "yaml.duplicates", "src_type": "ip"}]
                    * 3
                ), (policy, schema_only, raw, batch_size, actual)

print("schema, selector, raw, schema_only, and duplicate keys: ok")
