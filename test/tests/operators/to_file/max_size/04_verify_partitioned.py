# runner: python
"""Verify partitioned max_size rotation preserves all 900 events
(300 per region) across the correct hive directories.

File counts per directory are intentionally not asserted — see
`02_verify_single_partition.py` for the overshoot caveat.
"""

import json
import os
from pathlib import Path


def main() -> None:
    root = Path(os.environ["FILE_ROOT"]) / "out" / "maxsize_part"
    dirs = sorted(d.name for d in root.iterdir() if d.is_dir())
    print(f"partition_dirs: {dirs}")

    total_rows = 0
    ids = set()
    for d in sorted(root.iterdir()):
        if not d.is_dir():
            continue
        files = list(d.glob("*.json"))
        rows = 0
        for f in files:
            with open(f) as fh:
                for line in fh:
                    line = line.strip()
                    if line:
                        obj = json.loads(line)
                        ids.add(obj["id"])
                        rows += 1
        total_rows += rows
        print(f"{d.name}: rows={rows}")

    print(f"total_rows: {total_rows}")
    print(f"all_ids_present: {ids == set(range(900))}")


if __name__ == "__main__":
    main()
