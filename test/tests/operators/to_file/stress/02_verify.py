# runner: python
"""Verify the total number of written events across all partitions is 1M.

Per-partition row counts are non-deterministic (driven by `random()`), so
only the aggregate and the presence of every partition are checked.
"""

import json
import os
from pathlib import Path


def main() -> None:
    root = Path(os.environ["FILE_ROOT"]) / "out" / "stress"
    dirs = sorted(d.name for d in root.iterdir() if d.is_dir())
    print(f"partition_dirs: {dirs}")

    total_rows = 0
    all_nonempty = True
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
                        json.loads(line)
                        rows += 1
        if rows == 0:
            all_nonempty = False
        total_rows += rows

    print(f"total_rows: {total_rows}")
    print(f"all_partitions_nonempty: {all_nonempty}")


if __name__ == "__main__":
    main()
