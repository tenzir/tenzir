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
        'export\nwhere value == "unbuffered"\nto_stdout { write_ndjson }\n'
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
    assert "`secret` cannot be imported as secrets" in result.stderr.decode()
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
    print("ok: unbuffered import persists events")
finally:
    node.stop()
