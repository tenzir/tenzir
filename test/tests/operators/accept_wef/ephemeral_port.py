# runner: python
# timeout: 180

"""Subscriptions name the port that the OS picks for an endpoint with port 0.

Without `public_url`, the delivery address combines the host from the client's
`a:To` with the port that the collector listens on.
"""

from __future__ import annotations

import re
import shutil
import signal
import tempfile
import threading
import time
from pathlib import Path
from urllib.parse import urlsplit

from fixtures.wef_client import (
    WefClient,
    collector_pipeline,
    event_xml,
    remove_pki,
    start_collector,
    temporary_pki,
)


def main() -> None:
    pki = temporary_pki()
    state = Path(tempfile.mkdtemp(prefix="wef-state-"))
    # The test stops the collector itself, because a pipeline that ends with
    # the delivered event may cut off the response to it.
    process = start_collector(
        collector_pipeline(0, pki), state, env={"TENZIR_CONSOLE_VERBOSITY": "debug"}
    )
    assert process.stderr is not None
    stderr_pipe = process.stderr
    process.stderr = None  # The log reader owns the pipe, not communicate().
    listening = threading.Event()
    ports: list[int] = []
    stderr_lines: list[str] = []

    def read_logs() -> None:
        with stderr_pipe:
            for line in stderr_pipe:
                stderr_lines.append(line)
                if match := re.search(r"accept_wef listening on port (\d+)", line):
                    ports.append(int(match.group(1)))
                    listening.set()

    log_reader = threading.Thread(target=read_logs, daemon=True)
    log_reader.start()
    try:
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline and not listening.wait(0.1):
            if process.poll() is not None:
                raise AssertionError("collector exited before reporting its port")
        assert len(ports) == 1, "collector did not report its port"
        client = WefClient("127.0.0.1", ports[0], pki)
        client.wait_until_listening(process=process)
        client.enumerate()
        address = client.subscriptions["security"].address
        print("advertised port matches:", urlsplit(address).port == ports[0])
        response = client.events("security", [event_xml(record=1)])
        print("delivery:", response.status)
        client.close()
        process.send_signal(signal.SIGTERM)
        process.communicate(timeout=60)
        log_reader.join(timeout=5)
        assert process.returncode == 0, "".join(stderr_lines)
    except Exception as error:
        if process.poll() is None:
            process.kill()
        process.communicate(timeout=10)
        log_reader.join(timeout=5)
        error.add_note(f"collector stderr:\n{''.join(stderr_lines)}")
        raise
    finally:
        if process.poll() is None:
            process.kill()
            process.wait()
        remove_pki(pki)
        shutil.rmtree(state, ignore_errors=True)


if __name__ == "__main__":
    main()
