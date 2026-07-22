# fixtures: [node]
# timeout: 30

"""Verify that regular live exports retain flush-time delivery."""

from __future__ import annotations

import json
import select
import shlex
import subprocess
import time


def start_pipeline(env: dict[str, str], source: str) -> subprocess.Popen[str]:
    return subprocess.Popen(
        [
            *shlex.split(env["TENZIR_NODE_CLIENT_BINARY"]),
            "--bare-mode",
            "--console-verbosity=warning",
            "--multi",
            f"--endpoint={env['TENZIR_NODE_CLIENT_ENDPOINT']}",
            source,
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def read_line(process: subprocess.Popen[str], timeout: float) -> str | None:
    assert process.stdout is not None
    readable, _, _ = select.select([process.stdout], [], [], timeout)
    return process.stdout.readline() if readable else None


def stop(process: subprocess.Popen[str]) -> None:
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()


node = acquire_fixture("node")
node.start()
processes: list[subprocess.Popen[str]] = []

try:
    live = start_pipeline(
        node.env,
        'export live=true\nwhere value == "live"\nwrite_ndjson\n',
    )
    importer = start_pipeline(node.env, "read_ndjson\nimport\n")
    processes.extend([live, importer])
    time.sleep(0.2)
    assert importer.stdin is not None
    importer.stdin.write(json.dumps({"value": "live"}) + "\n")
    importer.stdin.flush()

    assert read_line(live, 0.5) is None, "live export bypassed the import buffer"
    event = read_line(live, 5)
    assert event and json.loads(event)["value"] == "live"
    print("ok: regular live exports retain flush-time delivery")
finally:
    for process in reversed(processes):
        stop(process)
    node.stop()
