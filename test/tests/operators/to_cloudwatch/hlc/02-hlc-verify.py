# runner: python

from __future__ import annotations

import json
import os
from pathlib import Path


def main() -> None:
    requests = [
        json.loads(line)
        for line in Path(os.environ["CLOUDWATCH_HLC_RECORDS"]).read_text().splitlines()
    ]
    messages = [
        event["message"] for request in requests for event in request["body"]["events"]
    ]
    print(f"request_count: {len(requests)}")
    print(f"token_ok: {requests[0]['authorization'] == 'Bearer tenzir-test-token'}")
    print(f"messages_ok: {messages == ['hlc-one']}")


if __name__ == "__main__":
    main()
