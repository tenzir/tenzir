# runner: python
"""A live read ends cleanly when the pipeline stops.

Live mode used to keep polling after the pipeline stopped, until the pipeline
gave up on it and reported that it was aborted. The test waits until the
operator polls, sends the signal that starts a graceful shutdown, and expects a
clean exit.
"""

from __future__ import annotations

import os
import shlex
import shutil
import signal
import subprocess
import tempfile
import time
from pathlib import Path

PIPELINE = """\
from_microsoft_sql table=env("MSSQL_LIVE_STREAM_TABLE"),
  live=true,
  host=env("MSSQL_HOST"),
  port=int(env("MSSQL_PORT")),
  user=env("MSSQL_USER"),
  password=env("MSSQL_PASSWORD"),
  database=env("MSSQL_DATABASE")
where payload == env("MSSQL_LIVE_STREAM_TOKEN")
discard
"""


def _resolve_tenzir_binary() -> tuple[str, ...]:
    env_val = os.environ.get("TENZIR_BINARY")
    if env_val:
        return tuple(shlex.split(env_val))
    which_result = shutil.which("tenzir")
    if which_result:
        return (which_result,)
    raise RuntimeError("tenzir executable not found")


def main() -> None:
    tenzir = _resolve_tenzir_binary()
    with tempfile.TemporaryDirectory(prefix="mssql-stop-") as tmpdir:
        pipe_path = Path(tmpdir) / "pipe.tql"
        pipe_path.write_text(PIPELINE, encoding="utf-8")
        process = subprocess.Popen(
            [
                *tenzir,
                "--bare-mode",
                "--console-verbosity=warning",
                "-f",
                str(pipe_path),
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=os.environ.copy(),
        )
        # The operator polls every second; wait for a few polls.
        time.sleep(3)
        assert process.poll() is None, process.communicate()
        process.send_signal(signal.SIGTERM)
        _, stderr = process.communicate(timeout=30)
    assert "internal error" not in stderr, stderr
    assert process.returncode == 0, f"rc={process.returncode}\n{stderr}"
    print("ok")


if __name__ == "__main__":
    main()
