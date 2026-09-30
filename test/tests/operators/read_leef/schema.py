# runner: python

import json
import os
from pathlib import Path
import shlex
import subprocess


binary = [*shlex.split(os.environ["TENZIR_BINARY"]), "--bare-mode", "--nova=true"]
schemas = Path(__file__).parent / "schemas"
header = b"LEEF:2.0|001|false|192.0.2.1|1s|^|"
metadata = {
    "vendor": "001",
    "product_name": "false",
    "product_version": "192.0.2.1",
    "event_class_id": "1s",
}
metadata_projection = ", ".join(
    f"{key}, {key}_type=type_of({key}).kind" for key in metadata
)
expected_metadata = {**metadata, **{f"{key}_type": "string" for key in metadata}}

for option in ('schema="leef.first"', 'selector="attributes.kind:leef"'):
    for schema_only in (False, True):
        for raw in (False, True):
            projection = "attributes, schema=@name"
            if not schema_only:
                projection += f", {metadata_projection}"
            result = subprocess.run(
                [
                    *binary,
                    f"--schema-dirs={schemas}",
                    f"load_stdin | read_leef {option}, "
                    f"schema_only={str(schema_only).lower()}, raw={str(raw).lower()} "
                    f"| select {projection} | write_ndjson",
                ],
                input=header + b"kind=first^value=42^text=001^extra=123^flag=true\n",
                capture_output=True,
                timeout=20,
            )
            assert result.returncode == 0 and not result.stderr, result.stderr
            actual = json.loads(result.stdout)
            expected = {
                "attributes": {
                    "kind": "first",
                    "value": 42,
                    "text": "001",
                    **(
                        {}
                        if schema_only
                        else {
                            "extra": "123" if raw else 123,
                            "flag": "true" if raw else True,
                        }
                    ),
                },
                "schema": "leef.first",
                **({} if schema_only else expected_metadata),
            }
            assert actual == expected, (option, schema_only, raw, actual)

# A missing non-strict schema infers attributes, never explicit header strings.
for option in ('schema="missing"', 'selector="attributes.kind:missing"'):
    for raw in (False, True):
        result = subprocess.run(
            [
                *binary,
                f"load_stdin | read_leef {option}, raw={str(raw).lower()} "
                f"| select attributes, {metadata_projection} | write_ndjson",
            ],
            input=header + b"kind=first^value=42^text=001^flag=true\n",
            capture_output=True,
            timeout=20,
        )
        assert result.returncode == 0 and b"warning:" in result.stderr, result.stderr
        actual = json.loads(result.stdout)
        assert actual == {
            "attributes": {
                "kind": "first",
                "value": "42" if raw else 42,
                "text": "001" if raw else 1,
                "flag": "true" if raw else True,
            },
            **expected_metadata,
        }, (option, raw, actual)

print("schema, selector, raw, and schema_only: ok")
