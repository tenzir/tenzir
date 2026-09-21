import json
import os
from pathlib import Path


requests = [
    json.loads(line)
    for line in Path(os.environ["GCL_CAPTURE_FILE"]).read_text().splitlines()
]
assert sum(len(request["entries"]) for request in requests) == 10000, requests
assert len(requests) == 100, len(requests)
assert max(len(request["entries"]) for request in requests) == 100, requests
