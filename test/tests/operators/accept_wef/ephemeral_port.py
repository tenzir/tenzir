# runner: python
# timeout: 180

"""Subscriptions name the port that the OS picks for an endpoint with port 0.

Without `public_url`, the delivery address combines the host from the client's
`a:To` with the port that the collector listens on.
"""

from __future__ import annotations

import os
import shutil
import signal
import subprocess
import tempfile
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


def listening_ports(pid: int) -> list[int]:
    """Returns the ports that a process listens on at 127.0.0.1."""
    proc = Path(f"/proc/{pid}")
    if proc.exists():
        inodes = set()
        for fd in (proc / "fd").iterdir():
            try:
                target = os.readlink(fd)
            except OSError:
                continue
            if target.startswith("socket:["):
                inodes.add(target[8:-1])
        ports = []
        for line in (proc / "net" / "tcp").read_text().splitlines()[1:]:
            fields = line.split()
            address, port = fields[1].split(":")
            # 0100007F is 127.0.0.1, and state 0A is LISTEN.
            if address == "0100007F" and fields[3] == "0A" and fields[9] in inodes:
                ports.append(int(port, 16))
        return ports
    output = subprocess.run(
        ["lsof", "-a", "-p", str(pid), "-iTCP", "-sTCP:LISTEN", "-P", "-n", "-Fn"],
        capture_output=True,
        text=True,
    ).stdout
    return [
        int(line.rsplit(":", 1)[1])
        for line in output.splitlines()
        if line.startswith("n127.0.0.1:")
    ]


def main() -> None:
    pki = temporary_pki()
    state = Path(tempfile.mkdtemp(prefix="wef-state-"))
    # The test stops the collector itself, because a pipeline that ends with
    # the delivered event may cut off the response to it.
    process = start_collector(collector_pipeline(0, pki), state)
    try:
        deadline = time.monotonic() + 120
        ports: list[int] = []
        while not ports and time.monotonic() < deadline:
            assert process.poll() is None, process.stderr.read()
            ports = listening_ports(process.pid)
            time.sleep(0.1)
        assert len(ports) == 1, ports
        client = WefClient("127.0.0.1", ports[0], pki)
        client.wait_until_listening(process=process)
        client.enumerate()
        address = client.subscriptions["security"].address
        print("advertised port matches:", urlsplit(address).port == ports[0])
        response = client.events("security", [event_xml(record=1)])
        print("delivery:", response.status)
        client.close()
        process.send_signal(signal.SIGTERM)
        stdout, stderr = process.communicate(timeout=60)
        assert process.returncode == 0, stderr
    finally:
        if process.poll() is None:
            process.kill()
            process.wait()
        remove_pki(pki)
        shutil.rmtree(state, ignore_errors=True)


if __name__ == "__main__":
    main()
