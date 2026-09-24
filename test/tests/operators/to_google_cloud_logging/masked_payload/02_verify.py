import json
import os
from pathlib import Path


requests = [
    json.loads(line)
    for line in Path(os.environ["GCL_CAPTURE_FILE"]).read_text().splitlines()
]
entries = [entry for request in requests for entry in request["entries"]]
actual = [
    {
        "id": int(entry["json_payload"]["id"]),
        "empty_record": entry["json_payload"]["empty_record"],
        "empty_list": entry["json_payload"]["empty_list"],
        "resource_type": entry["resource"]["type"],
        "host": entry["resource"]["labels"]["host"],
    }
    for entry in entries
]
assert sorted(actual, key=lambda row: row["id"]) == [
    {
        "id": 1,
        "empty_record": {},
        "empty_list": [],
        "resource_type": "global",
        "host": "one",
    },
    {
        "id": 3,
        "empty_record": {},
        "empty_list": [],
        "resource_type": "generic_node",
        "host": "three",
    },
], actual
