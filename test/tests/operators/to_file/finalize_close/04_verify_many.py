# runner: python
"""Reaching this script means the pipeline did NOT hang — all 20 partitions
were closed and `partition_count_` reached 0. Verify every partition
directory has a non-empty, valid NDJSON file."""

import json
import os
from pathlib import Path


EXPECTED = set("abcdefghijklmnopqrst")


def main() -> None:
    root = Path(os.environ["FILE_ROOT"]) / "out" / "finalize" / "many"
    found = set()
    empty = []
    bad = []

    for d in sorted(root.iterdir()):
        if not d.is_dir():
            continue
        pk = d.name.split("=", 1)[1].strip('"')
        found.add(pk)
        for f in d.glob("*.json"):
            if f.stat().st_size == 0:
                empty.append(pk)
                continue
            for line in f.read_text().splitlines():
                if not line.strip():
                    continue
                try:
                    json.loads(line)
                except json.JSONDecodeError:
                    bad.append(pk)

    missing = EXPECTED - found
    print(f"partitions: {len(found)}")
    print(f"all_present: {not missing}")
    print(f"empty_files: {len(empty)}")
    print(f"bad_json: {len(bad)}")
    if missing:
        print(f"  missing: {sorted(missing)}")


if __name__ == "__main__":
    main()
