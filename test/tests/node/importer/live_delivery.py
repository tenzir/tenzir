"""Verify that regular live exports retain flush-time delivery."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
import json
import os
import select
import shlex
import subprocess
import time


line_buffers: dict[int, bytearray] = {}


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
    buffer = line_buffers.setdefault(process.pid, bytearray())
    deadline = time.monotonic() + timeout
    while True:
        if (newline := buffer.find(b"\n")) >= 0:
            line = bytes(buffer[: newline + 1])
            del buffer[: newline + 1]
            return line.decode()
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return None
        readable, _, _ = select.select([process.stdout], [], [], remaining)
        if not readable:
            return None
        if not (chunk := os.read(process.stdout.fileno(), 4096)):
            return None
        buffer.extend(chunk)


def stop(process: subprocess.Popen[str]) -> None:
    line_buffers.pop(process.pid, None)
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()


def write_event(process: subprocess.Popen[str], event: dict[str, object]) -> None:
    assert process.stdin is not None
    process.stdin.write(json.dumps(event) + "\n")
    process.stdin.flush()


def activity_sentinel(pipeline_id: str, timestamp: datetime) -> dict[str, object]:
    return {
        "schema": "tenzir.metrics.pipeline",
        "internal": True,
        "pipeline_id": pipeline_id,
        "timestamp": timestamp.isoformat().replace("+00:00", "Z"),
        "ingress": {"internal": True, "bytes": 0},
        "egress": {"internal": True, "bytes": 0},
    }


def wait_for_activity(
    process: subprocess.Popen[str], pipeline_id: str, timeout: float
) -> bool:
    deadline = time.monotonic() + timeout
    while (remaining := deadline - time.monotonic()) > 0:
        line = read_line(process, remaining)
        if line and json.loads(line)["pipelines"]["id"] == pipeline_id:
            return True
    return False


node = acquire_fixture("node")
node.start()
processes: list[subprocess.Popen[str]] = []

try:
    seed = start_pipeline(
        node.env,
        'from {value: "ready"}\n@name = "live-delivery"\nimport\n',
    )
    _, stderr = seed.communicate(timeout=5)
    assert seed.returncode == 0, stderr

    live = start_pipeline(
        node.env,
        'export live=true, retro=true\nwhere @name == "live-delivery"\nwrite_ndjson\n',
    )
    activity = start_pipeline(
        node.env,
        "pipeline_activity range=10s, interval=10s\nunroll pipelines\nwrite_ndjson\n",
    )
    processes.extend([live, activity])
    ready = read_line(live, 5)
    assert ready and json.loads(ready)["value"] == "ready"

    importer = start_pipeline(
        node.env,
        "read_ndjson\n"
        "timestamp = time(timestamp)\n"
        "ingress.bytes = ingress.bytes.uint()\n"
        "egress.bytes = egress.bytes.uint()\n"
        "@name = schema\n"
        "@internal = internal\n"
        "drop schema, internal\n"
        "import\n",
    )
    processes.append(importer)
    timestamp = datetime.now(UTC) + timedelta(seconds=20)
    for attempt in range(10):
        ready_id = f"live-delivery-ready-{attempt}"
        write_event(importer, activity_sentinel(ready_id, timestamp))
        if wait_for_activity(activity, ready_id, 0.5):
            break
        timestamp += timedelta(seconds=20)
    else:
        raise AssertionError("pipeline activity subscriber did not become ready")

    write_event(
        importer,
        {
            "schema": "live-delivery",
            "internal": False,
            "value": "live",
        },
    )
    acknowledgement_id = "live-delivery-ack"
    write_event(
        importer,
        activity_sentinel(acknowledgement_id, timestamp + timedelta(seconds=20)),
    )

    assert wait_for_activity(activity, acknowledgement_id, 5), (
        "importer did not acknowledge the event"
    )
    assert read_line(live, 0.5) is None, "live export bypassed the import buffer"
    event = read_line(live, 10)
    assert event and json.loads(event)["value"] == "live"
    print("ok: regular live exports retain flush-time delivery")
finally:
    for process in reversed(processes):
        stop(process)
    node.stop()
