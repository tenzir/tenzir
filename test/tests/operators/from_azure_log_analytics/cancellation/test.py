# runner: python
# timeout: 30

from __future__ import annotations

import json
import os
import shlex
import signal
import subprocess
import urllib.request

pipeline = """
from_azure_log_analytics env("AZLOG_QUERY"),
  workspace_id=env("AZLOG_WORKSPACE"),
  azure_auth={
    tenant_id: "fixture-tenant",
    client_id: "fixture-client",
    client_secret: "fixture-secret",
    authority: env("AZLOG_AUTHORITY"),
  },
  _base_url=env("AZLOG_URL"),
  tls=false
"""
process = subprocess.Popen(
    [*shlex.split(os.environ["TENZIR_BINARY"]), "--bare-mode", pipeline],
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
    text=True,
)
try:
    # The fixture holds this control request until the query arrives. There is
    # no polling or sleep, and the query response stays blocked until teardown.
    with urllib.request.urlopen(
        os.environ["AZLOG_URL"] + "/ready", timeout=12
    ) as ready:
        assert json.load(ready)["ready"], "query did not reach the fixture"
    process.send_signal(signal.SIGINT)
    stdout, stderr = process.communicate(timeout=5)
    assert not stdout, stdout
    # The executor may report an aborted pipeline, but cancellation must not
    # turn into a connector error about exhausted HTTP retries.
    assert "Azure Log Analytics request failed" not in stderr, stderr
    assert "DO-NOT-LOG" not in stderr, stderr
    assert process.returncode in (0, 1, 130), (process.returncode, stderr)
    print("in-flight query cancelled without waiting for a response")
finally:
    if process.poll() is None:
        process.kill()
        process.wait(timeout=5)
