# runner: python
# timeout: 60

"""Only one pipeline at a time may use a bookmark store."""

from __future__ import annotations

import shutil
import tempfile
from pathlib import Path

from fixtures.wef_client import (
    free_port,
    WefClient,
    collector_pipeline,
    remove_pki,
    start_collector,
    temporary_pki,
)


def main() -> None:
    pki = temporary_pki()
    state = Path(tempfile.mkdtemp(prefix="wef-state-"))
    first_port, second_port = free_port(), free_port()
    options = ',\n  state="shared"'
    first = start_collector(collector_pipeline(first_port, pki, options=options), state)
    try:
        WefClient("127.0.0.1", first_port, pki).wait_until_listening(process=first)
        second = start_collector(
            collector_pipeline(second_port, pki, options=options), state
        )
        _, stderr = second.communicate(timeout=30)
        print("second pipeline failed:", second.returncode != 0)
        print("reported the lock:", "another pipeline uses the state" in stderr)
        # Without an explicit state, the default derives from the endpoint.
        third = start_collector(collector_pipeline(second_port, pki), state)
        try:
            WefClient("127.0.0.1", second_port, pki).wait_until_listening(process=third)
            print("distinct state starts:", third.poll() is None)
        finally:
            third.kill()
            third.wait()
    finally:
        first.kill()
        first.wait()
        remove_pki(pki)
        shutil.rmtree(state, ignore_errors=True)


if __name__ == "__main__":
    main()
