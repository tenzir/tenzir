# runner: python
"""Each region partition should hold all 200 of its events."""

import json
import os
from pathlib import Path


def main() -> None:
    root = Path(os.environ["FILE_ROOT"]) / "out" / "real_time_partitioned"
    totals = {}
    for d in root.iterdir():
        if not d.is_dir():
            continue
        count = 0
        for f in d.glob("*.json"):
            with open(f) as fh:
                for line in fh:
                    if line.strip():
                        count += 1
        totals[d.name] = count
    for k in sorted(totals):
        print(f"{k}: {totals[k]}")


if __name__ == "__main__":
    main()
