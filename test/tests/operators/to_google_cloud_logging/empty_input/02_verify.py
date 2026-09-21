import json
import os
from pathlib import Path


requests = [
    json.loads(line)
    for line in Path(os.environ["GCL_CAPTURE_FILE"]).read_text().splitlines()
]
assert requests == [], requests
