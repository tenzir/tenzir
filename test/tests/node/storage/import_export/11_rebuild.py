# timeout: 240

"""Verify rebuild treats every Nova child as an ordinary partition."""

from __future__ import annotations

import json
import shlex
import subprocess
from pathlib import Path

node = acquire_fixture("node")
node.start()
binary = shlex.split(node.env["TENZIR_NODE_CLIENT_BINARY"])[0]
endpoint = f"--endpoint={node.env['TENZIR_NODE_CLIENT_ENDPOINT']}"
common = ["--bare-mode", "--console-verbosity=error", endpoint]
rebuild = subprocess.run(
    [str(Path(binary).with_name("tenzir-ctl")), *common, "rebuild", "--all"],
    capture_output=True,
    check=False,
    timeout=180,
)
assert rebuild.returncode == 0, rebuild.stderr.decode()


def export(name: str) -> list[dict[str, object]]:
    result = subprocess.run(
        [
            binary,
            "--neo",
            "--nova",
            *common,
            f'export\nwhere @name == "{name}"\nwrite_ndjson',
        ],
        capture_output=True,
        check=False,
        timeout=45,
    )
    assert result.returncode == 0, result.stderr.decode()
    return [json.loads(line) for line in result.stdout.splitlines()]


assert sorted(event["id"] for event in export("test.events")) == [1, 2, 3, 4]
assert export("test.nullonly") == [{"x": None}]
print("ok: Nova child partitions rebuild")
node.stop()
