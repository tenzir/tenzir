import json
import os
from pathlib import Path


path = Path(os.environ["FILE_ROOT"]) / "out/roundtrip.json"
values = [json.loads(line)["x"] for line in path.read_text().splitlines()]
assert sorted(values) == list(range(500)), values
