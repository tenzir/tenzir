# timeout: 180

"""Check buffered live delivery, active snapshots, and internal events."""

from __future__ import annotations

import json
import os
import select
import shlex
import subprocess
import time


def command(env: dict[str, str], pipeline: str) -> list[str]:
    return [
        *shlex.split(env["TENZIR_NODE_CLIENT_BINARY"]),
        "--neo",
        "--nova=true",
        "--bare-mode",
        "--console-verbosity=error",
        f"--endpoint={env['TENZIR_NODE_CLIENT_ENDPOINT']}",
        pipeline,
    ]


def import_event(env: dict[str, str], number: int) -> None:
    value = "null" if number == 1 else '"concrete"'
    result = subprocess.run(
        command(
            env,
            f'from {{id: {number}, value: {value}}}\n@name = "live-delivery"\nimport',
        ),
        capture_output=True,
        check=False,
        timeout=45,
    )
    assert result.returncode == 0, result.stderr.decode()


def read_event(process: subprocess.Popen[bytes], timeout: float) -> dict[str, object]:
    assert process.stdout is not None
    deadline = time.monotonic() + timeout
    buffer = bytearray()
    while time.monotonic() < deadline:
        remaining = deadline - time.monotonic()
        readable, _, _ = select.select([process.stdout], [], [], remaining)
        if not readable:
            break
        chunk = os.read(process.stdout.fileno(), 4096)
        if not chunk:
            break
        buffer.extend(chunk)
        if b"\n" in buffer:
            line, _, remainder = buffer.partition(b"\n")
            assert not remainder, f"unexpected extra event: {remainder!r}"
            return json.loads(line)
    raise AssertionError("timed out waiting for Nova export event")


node = acquire_fixture("node")
node.start()
live: subprocess.Popen[bytes] | None = None
streaming_import: subprocess.Popen[bytes] | None = None
internal_live: subprocess.Popen[bytes] | None = None
try:
    import_event(node.env, 1)
    live = subprocess.Popen(
        command(
            node.env,
            'export live=true, retro=true\nwhere @name == "live-delivery"\n'
            "to_stdout { write_ndjson }",
        ),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    assert read_event(live, 45) == {"id": 1, "value": None}
    seed = subprocess.run(
        command(
            node.env,
            'from {id: 0}\n@name = "native-ready"\n@internal = true\nimport',
        ),
        capture_output=True,
        check=False,
        timeout=45,
    )
    assert seed.returncode == 0, seed.stderr.decode()
    internal_live = subprocess.Popen(
        command(
            node.env,
            "export internal=true, live=true, retro=true\n"
            'where @name == "native-ready" or '
            '@name == "tenzir.metrics.operator.native-test"\n'
            "to_stdout { write_ndjson }",
        ),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    assert read_event(internal_live, 45) == {"id": 0}
    streaming_import = subprocess.Popen(
        command(
            node.env,
            "from_stdin { read_json _batch_size=1 }\n"
            "@name = schema\n@internal = internal\ndrop schema, internal\nimport",
        ),
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    assert streaming_import.stdin is not None
    streaming_import.stdin.write(
        b'{"schema":"tenzir.metrics.operator.native-test","internal":true,'
        b'"id":3,"values":[1,"two"]}\n'
    )
    streaming_import.stdin.flush()
    assert read_event(internal_live, 45) == {"id": 3, "values": [1, "two"]}
    # The sentinel confirms both the importer and its live subscription are ready.
    streaming_import.stdin.write(
        b'{"schema":"live-delivery","internal":false,"id":2,"value":"concrete"}\n'
    )
    streaming_import.stdin.flush()
    assert live.stdout is not None
    assert not select.select([live.stdout], [], [], 0.5)[0], (
        "live export bypassed the import buffer"
    )
    assert read_event(live, 45) == {"id": 2, "value": "concrete"}
    snapshot = subprocess.run(
        command(
            node.env,
            'export\nwhere @name == "live-delivery" and id == 2\nhead 1\nwrite_ndjson',
        ),
        capture_output=True,
        check=False,
        timeout=45,
    )
    assert snapshot.returncode == 0, snapshot.stderr.decode()
    assert [json.loads(line) for line in snapshot.stdout.splitlines()] == [
        {"id": 2, "value": "concrete"}
    ]
    empty = subprocess.run(
        command(node.env, "export live=true\nhead 0\nwrite_ndjson"),
        capture_output=True,
        check=False,
        timeout=15,
    )
    assert empty.returncode == 0, empty.stderr.decode()
    assert not empty.stdout
    assert live.stdout is not None
    assert not select.select([live.stdout], [], [], 1)[0], "duplicate live event"
    print("ok: buffered live delivery, active snapshots, and internal events")
finally:
    if streaming_import is not None:
        assert streaming_import.stdin is not None
        streaming_import.stdin.close()
        try:
            streaming_import.wait(timeout=45)
        except subprocess.TimeoutExpired:
            streaming_import.kill()
            streaming_import.wait()
    for process in [internal_live, live]:
        if process is None:
            continue
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
    node.stop()
