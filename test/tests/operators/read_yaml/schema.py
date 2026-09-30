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

print("schema, selector, raw, and schema_only: ok")
