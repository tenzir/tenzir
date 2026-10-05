# runner: python
# timeout: 120

"""Bookmarks survive a graceful stop and a crash of the collector."""

from __future__ import annotations

import signal
import shutil
import tempfile
from pathlib import Path

from fixtures.wef_client import (
    free_port,
    WefClient,
    collector_pipeline,
    event_xml,
    remove_pki,
    start_collector,
    temporary_pki,
)


def run(pki, port: int, state: Path, steps) -> tuple[int, str]:
    process = start_collector(collector_pipeline(port, pki), state)
    client = WefClient("127.0.0.1", port, pki)
    try:
        client.wait_until_listening(process=process)
        steps(client, process)
        stdout, stderr = process.communicate(timeout=30)
        return process.returncode, stderr
    finally:
        client.close()
        if process.poll() is None:
            process.kill()
            process.wait()


def main() -> None:
    pki = temporary_pki()
    state = Path(tempfile.mkdtemp(prefix="wef-state-"))
    port = free_port()
    try:

        def first(client: WefClient, process) -> None:
            client.enumerate()
            print("initial bookmark:", client.subscriptions["security"].bookmark)
            for record in (1, 2):
                response = client.events(
                    "security",
                    [event_xml(record=record)],
                    bookmark={"Security": record},
                )
                assert response.status == 200, response.status
            # The ack means that the bookmark reached the disk.
            process.send_signal(signal.SIGTERM)

        code, stderr = run(pki, port, state, first)
        assert code == 0, stderr

        def second(client: WefClient, process) -> None:
            client.enumerate()
            print("after a graceful stop:", client.subscriptions["security"].bookmark)
            response = client.events(
                "security", [event_xml(record=3)], bookmark={"Security": 3}
            )
            assert response.status == 200, response.status
            process.kill()

        run(pki, port, state, second)

        def third(client: WefClient, process) -> None:
            client.enumerate()
            print("after a crash:", client.subscriptions["security"].bookmark)
            process.send_signal(signal.SIGTERM)

        code, stderr = run(pki, port, state, third)
        assert code == 0, stderr
    finally:
        remove_pki(pki)
        shutil.rmtree(state, ignore_errors=True)


if __name__ == "__main__":
    main()
