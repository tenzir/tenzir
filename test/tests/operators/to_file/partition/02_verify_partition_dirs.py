# runner: python
"""Verify hive-partitioned directory structure and event counts."""

import json
import os
from pathlib import Path


def main() -> None:
    root = Path(os.environ["FILE_ROOT"]) / "out" / "partitioned"
    # Collect all partition directories.
    dirs = sorted(d.name for d in root.iterdir() if d.is_dir())
    print(f"partition_dirs: {dirs}")

    # Count events per partition by reading all NDJSON files.
    totals = {}
    for d in root.iterdir():
        if not d.is_dir():
            continue
        count = 0
        for f in d.glob("*.json"):
            with open(f) as fh:
                for line in fh:
                    line = line.strip()
                    if line:
                        count += 1
        totals[d.name] = count

    for k in sorted(totals):
        print(f"{k}: {totals[k]}")


if __name__ == "__main__":
    main()
