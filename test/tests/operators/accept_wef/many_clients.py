# runner: python
# timeout: 180

"""Many clients forward compressed batches concurrently."""

from __future__ import annotations

import http.client
import shutil
import ssl
import tempfile
import threading
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

CLIENTS = 32
BATCHES = 5
EVENTS = 20


def forward(pki, port: int, index: int, errors: list[str]) -> None:
    name = f"client-{index:02}.example.org"
    client = WefClient("127.0.0.1", port, pki, name)
    try:
        client.wait_until_listening()
        client.enumerate()
        for batch in range(BATCHES):
            records = range(batch * EVENTS + 1, (batch + 1) * EVENTS + 1)
            events = [event_xml(record=record, computer=name) for record in records]
            try:
                response = client.events(
                    "security",
                    events,
                    bookmark={"Security": records[-1]},
                    compression="sldc1",
                )
            except (OSError, http.client.HTTPException, ssl.SSLError):
                # The pipeline stops after the last event, so the ack of the
                # last batch may not arrive.
                if batch == BATCHES - 1:
                    return
                raise
            if response.status != 200:
                errors.append(f"{name}: batch {batch} got {response.status}")
                return
    except Exception as exc:  # noqa: BLE001
        errors.append(f"{name}: {exc!r}")
    finally:
        client.close()


def main() -> None:
    pki = temporary_pki()
    state = Path(tempfile.mkdtemp(prefix="wef-state-"))
    port = free_port()
    for index in range(CLIENTS):
        pki.client(f"client-{index:02}.example.org")
    total = CLIENTS * BATCHES * EVENTS
    pipeline = collector_pipeline(
        port,
        pki,
        rest=(
            f"head {total}\n"
            "summarize events=count(), clients=count_distinct(wef.client), "
            "records=count_distinct(data.parse_winlog().System.EventRecordID)"
        ),
    )
    process = start_collector(pipeline, state)
    errors: list[str] = []
    try:
        threads = [
            threading.Thread(target=forward, args=(pki, port, index, errors))
            for index in range(CLIENTS)
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        stdout, stderr = process.communicate(timeout=60)
        assert not errors, errors
        assert process.returncode == 0, stderr
        print(stdout.strip())
    finally:
        if process.poll() is None:
            process.kill()
            process.wait()
        remove_pki(pki)
        shutil.rmtree(state, ignore_errors=True)


if __name__ == "__main__":
    main()
