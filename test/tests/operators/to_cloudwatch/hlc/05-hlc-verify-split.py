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
    ok_requests = [request for request in requests if request["status"] == 200]
    messages = [
        event["message"]
        for request in ok_requests
        for event in request["body"]["events"]
    ]
    print(f"split_request_count: {len(ok_requests)}")
    print(
        "split_messages_ok: "
        f"{messages == ['hlc-one', 'hlc-split-1', 'hlc-split-2', 'hlc-split-3']}"
    )


if __name__ == "__main__":
    main()
