# runner: python

import json
import os
from pathlib import Path
import shlex
import subprocess


binary = [*shlex.split(os.environ["TENZIR_BINARY"]), "--bare-mode", "--nova=true"]
schemas = Path(__file__).parent / "schemas"
line = "LEEF:2.0|001|false|192.0.2.1|1s|^|kind=first^value=42^text=001^extra=123"
metadata = {
    "leef_version": "2.0",
    "vendor": "001",
    "product_name": "false",
    "product_version": "192.0.2.1",
    "event_class_id": "1s",
}

for policy in ('schema="leef.first"', 'selector="attributes.kind:leef"'):
    for schema_only in (False, True):
        for raw in (False, True):
            pipeline = (
                f"from {{x: {json.dumps(line)}}} "
                f"| this = x.parse_leef({policy}, "
                f"schema_only={str(schema_only).lower()}, raw={str(raw).lower()}) "
                "| write_ndjson"
            )
            result = subprocess.run(
                [*binary, f"--schema-dirs={schemas}", pipeline],
                capture_output=True,
                timeout=20,
            )
            assert result.returncode == 0 and not result.stderr, result.stderr
            expected = {
                "attributes": {
                    "kind": "first",
                    "value": 42,
                    "text": "001",
                    **({} if schema_only else {"extra": "123" if raw else 123}),
                },
                **({} if schema_only else metadata),
            }
            actual = json.loads(result.stdout)
            assert actual == expected, (policy, schema_only, raw, actual)

print("LEEF function schema and selector policies passed")
