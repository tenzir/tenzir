# fixtures: [node]
# timeout: 30

"""Verify that an unbuffered importer still retains imported events."""

from __future__ import annotations

import json


node = acquire_fixture("node")
node.start()
tenzir = Executor.from_env(node.env)

try:
    result = tenzir.run('from {value: "unbuffered"}\nimport\n')
    assert result.returncode == 0, result.stderr.decode()
    result = tenzir.run('export\nwhere value == "unbuffered"\nwrite_ndjson\n')
    assert result.returncode == 0, result.stderr.decode()
    assert json.loads(result.stdout.decode())["value"] == "unbuffered"
    print("ok: unbuffered import persists events")
finally:
    node.stop()
