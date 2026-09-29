import json
import os
from pathlib import Path
import select
import shlex
import subprocess
import time


binary = shlex.split(os.environ["TENZIR_BINARY"])
schemas = Path(__file__).parent / "schemas"
for schema_only in [False, True]:
    process = subprocess.Popen(
        [
            *binary,
            "--bare-mode",
            "--nova=true",
            "--console-verbosity=warning",
            f"--schema-dirs={schemas}",
            'load_stdin | read_ndjson schema="builder.a", '
            f"schema_only={str(schema_only).lower()}, _batch_timeout=1h "
            "| write_ndjson",
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        process.stdin.write(
            b'{"payload":{"x":1},"common":{"first":[1]},"items":[42]}\n'
        )
        process.stdin.flush()
        diagnostics = b""
        deadline = time.monotonic() + 20
        while diagnostics.count(b"warning:") < 3:
            remaining = max(0, deadline - time.monotonic())
            ready, _, _ = select.select([process.stderr], [], [], remaining)
            assert ready, f"expected insertion diagnostics before EOF: {diagnostics!r}"
            chunk = os.read(process.stderr.fileno(), 4096)
            assert chunk, f"parser exited before insertion diagnostics: {diagnostics!r}"
            diagnostics += chunk
        stdout, stderr = process.communicate(timeout=20)
        diagnostics += stderr
        assert process.returncode == 0, diagnostics
        assert diagnostics.count(b"warning:") == 3, diagnostics
        row = json.loads(stdout)
        assert row["payload"] == (None if schema_only else {"x": 1}), row
        assert row["common"]["first"] == (None if schema_only else [1]), row
        assert row["items"] == ([None] if schema_only else [42]), row
    finally:
        if process.poll() is None:
            process.kill()
            process.wait()
