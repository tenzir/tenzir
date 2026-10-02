# fixtures: [http_server]
# timeout: 60

import json
import os
import select
import shlex
import subprocess
import threading
import urllib.request


url = os.environ["HTTP_FIXTURE_URL"] + "/splunk/warnings"
pipeline = f"""
from_splunk {json.dumps(url)}, search="fixture",
  earliest="-15m", latest="-5m",
  headers={{Authorization: "Splunk test-token"}}, tls=false
head 1
write_ndjson
"""
process = subprocess.Popen(
    [
        *shlex.split(os.environ["TENZIR_BINARY"]),
        "--bare-mode",
        "--nova=true",
        pipeline,
    ],
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
    bufsize=0,
)


def stderr_ready():
    ready, _, _ = select.select([process.stderr], [], [], 10)
    assert ready, "expected a Splunk warning"


try:
    # Observe the initial response before permitting the warning-only frame.
    while True:
        stderr_ready()
        line = process.stderr.readline()
        assert line, "stream ended before the initial warning"
        if b"= note: search: fixture" in line:
            break
    with urllib.request.urlopen(url + "/start", timeout=10):
        pass
    prefix = b""
    while not prefix.endswith(b"WARN: first: "):
        stderr_ready()
        byte = process.stderr.read(1)
        assert byte, "stream ended before the first queued warning"
        prefix += byte
    # Backpressure stalls diagnostic emission, not the HTTP producer. Hold it
    # past the one-second batch deadline with the second warning still ready.
    threading.Event().wait(2)
    assert process.stderr.readline().endswith(b"\n")
    # Drain only the first diagnostic. The next warning blocks stderr again,
    # so the result must be flushed without waiting for a timer-only wakeup.
    ready, _, _ = select.select([process.stdout], [], [], 5)
    assert ready, "warning traffic starved the buffered result"
    assert json.loads(process.stdout.readline()) == {"number": 1}
    stdout, stderr = process.communicate(timeout=10)
    assert process.returncode == 0, stderr
    assert not stdout, stdout
finally:
    if process.poll() is None:
        process.kill()
        process.wait()
    with urllib.request.urlopen(url + "/finish", timeout=10):
        pass

print("warning traffic does not starve buffered results")
