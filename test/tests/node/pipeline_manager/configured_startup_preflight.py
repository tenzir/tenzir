# runner: python

from __future__ import annotations

import os
import shlex
import shutil
import subprocess
import tempfile
import time
from pathlib import Path


def _node_binary() -> list[str]:
    if env_value := os.environ.get("TENZIR_NODE_BINARY"):
        return shlex.split(env_value)
    if binary := shutil.which("tenzir-node"):
        return [binary]
    raise RuntimeError("tenzir-node executable not found")


with tempfile.TemporaryDirectory() as tmp:
    root = Path(tmp)
    marker = root / "started.ndjson"
    config = root / "tenzir.yaml"
    config.write_text(
        f"""
tenzir:
  pipelines:
    a_good:
      definition: |
        from {{x: 1}}
        to_file "{marker}" {{
          write_ndjson
        }}
    z_bad:
      definition: |
        this_operator_does_not_exist
        discard
""".lstrip(),
        encoding="utf-8",
    )
    proc = subprocess.Popen(
        [
            *_node_binary(),
            "--bare-mode",
            "--plugins=bundled",
            f"--config={config}",
            f"--state-directory={root / 'state'}",
            f"--cache-directory={root / 'cache'}",
            "--console-verbosity=error",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    deadline = time.monotonic() + 60
    while proc.poll() is None and time.monotonic() < deadline:
        time.sleep(0.1)
    if proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)
        stdout, stderr = proc.communicate()
        raise AssertionError(
            "node kept running despite invalid configured pipeline\n"
            f"stdout:\n{stdout}\n"
            f"stderr:\n{stderr}"
        )
    stdout, stderr = proc.communicate()
    assert proc.returncode != 0, (proc.returncode, stdout, stderr)
    assert not marker.exists(), (
        marker.read_text(encoding="utf-8") if marker.exists() else ""
    )
    assert "this_operator_does_not_exist" in stderr, stderr

print("configured-startup-preflight: ok")
