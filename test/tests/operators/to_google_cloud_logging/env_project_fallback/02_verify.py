import json
import os
from pathlib import Path


requests = [
    json.loads(line)
    for line in Path(os.environ["GCL_CAPTURE_FILE"]).read_text().splitlines()
]
assert sum(len(request["entries"]) for request in requests) == 1000, requests
assert {request["log_name"] for request in requests} == {
    "projects/test-project/logs/env-fallback-log"
}, requests
