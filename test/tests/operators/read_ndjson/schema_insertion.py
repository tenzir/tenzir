import json
import os
from pathlib import Path
import shlex
import subprocess


binary = shlex.split(os.environ["TENZIR_BINARY"])
schemas = Path(__file__).parent / "schemas"
source = (
    '{"extra":1,"items":[{"second":"two"},{"first":"3"}],'
    '"nested.second":"two","nested.first":"1","nested.first":"2",'
    '"common.text":"2001:0db8::1","common.first":"42",'
    '"common.extra":7,"kind":"a"}\n{}\n'
)
for schema_only in [False, True]:
    result = subprocess.run(
        [
            *binary,
            "--bare-mode",
            "--nova=true",
            f"--schema-dirs={schemas}",
            'load_stdin | read_ndjson schema="builder.a", '
            f'schema_only={str(schema_only).lower()}, unflatten_separator="." '
            "| write_ndjson",
        ],
        input=source,
        text=True,
        capture_output=True,
        timeout=20,
    )
    assert result.returncode == 0 and not result.stderr, result.stderr
    first, second = [json.loads(line) for line in result.stdout.splitlines()]
    fields = ["kind", "common", "payload", "nested", "items", "missing"]
    assert list(first) == fields + ([] if schema_only else ["extra"]), first
    assert first["nested"] == {"first": 2, "second": "two"}, first
    assert list(first["nested"]) == ["first", "second"], first
    assert first["items"] == [
        {"first": None, "second": "two"},
        {"first": 3, "second": None},
    ], first
    assert all(list(item) == ["first", "second"] for item in first["items"]), first
    assert first["common"] == {
        "first": 42,
        "text": "2001:0db8::1",
        **({} if schema_only else {"extra": 7}),
    }, first
    assert first["missing"] is None and first["payload"] is None, first
    assert second == dict.fromkeys(fields), second
    assert list(second) == fields, second
