"""Verify that an unbuffered importer still retains imported events."""

from __future__ import annotations

import json

node = acquire_fixture("node")
node.start()
tenzir = Executor.from_env(node.env)
tenzir.binary = (*tenzir.binary, "--nova=true")

try:
    result = tenzir.run('from {value: "unbuffered"}\nimport\n')
    assert result.returncode == 0, result.stderr.decode()
    result = tenzir.run(
        'export\nwhere value == "unbuffered" and '
        # The HMAC-SHA256 of "unbuffered" with the key "test-value".
        'hmac(value, secret("test-secret")) == '
        '"ca3fd7a666b44364cd54ec22ef40bef3dc721687f0d62e35bad70e06fbfe5d46"\n'
        "to_stdout { write_ndjson }\n"
    )
    assert result.returncode == 0, result.stderr.decode()
    assert json.loads(result.stdout.decode())["value"] == "unbuffered"
    result = tenzir.run(
        'from {value: "secret-import"}\n'
        'scalar = secret("test-secret")\n'
        'nested = {token: secret("test-secret")}\n'
        'items = [secret("test-secret"), "plain", null]\n'
        "import\n"
    )
    assert result.returncode == 0, result.stderr.decode()
    assert "secrets cannot be stored in events" in result.stderr.decode()
    result = tenzir.run(
        'export\nwhere value == "secret-import"\nto_stdout { write_ndjson }\n'
    )
    assert result.returncode == 0, result.stderr.decode()
    assert json.loads(result.stdout.decode()) == {
        "value": "secret-import",
        "scalar": "***",
        "nested": {"token": "***"},
        "items": ["***", "plain", None],
    }
    for schema, source in [
        ("tenzir.metrics.import-test", 'metrics "import-test"'),
        ("tenzir.diagnostic", "diagnostics"),
    ]:
        result = tenzir.run(
            'from {marker: "internal-operators", id: 0}\n'
            f"@name = {json.dumps(schema)}\n@internal = true\nimport\n"
        )
        assert result.returncode == 0, result.stderr.decode()
        result = tenzir.run(
            'from {marker: "internal-operators", id: 99}\n'
            f"@name = {json.dumps(schema)}\nimport\n"
        )
        assert result.returncode == 0, result.stderr.decode()
        result = tenzir.run(
            f'{source}\nwhere marker == "internal-operators"\n'
            "to_stdout { write_ndjson }\n"
        )
        assert result.returncode == 0, result.stderr.decode()
        assert json.loads(result.stdout.decode()) == {
            "marker": "internal-operators",
            "id": 0,
        }
    result = tenzir.run(
        'metrics\nwhere marker == "internal-operators"\nto_stdout { write_ndjson }\n'
    )
    assert result.returncode == 0, result.stderr.decode()
    assert [json.loads(line) for line in result.stdout.splitlines()] == [
        {"marker": "internal-operators", "id": 0}
    ]
    print("ok: unbuffered import persists events")
finally:
    node.stop()
