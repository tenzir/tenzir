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
    print(f"request_count_unchanged: {len(ok_requests) == 4}")
    print(f"oversized_skipped: {all(len(message) < 1048550 for message in messages)}")


if __name__ == "__main__":
    main()
