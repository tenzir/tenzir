# runner: python
"""Verify a large max_size rotation preserves all 5000 events without
duplication. File counts are not asserted — see
`02_verify_single_partition.py` for the overshoot caveat.
"""

import json
import os
from pathlib import Path


def main() -> None:
    root = Path(os.environ["FILE_ROOT"]) / "out" / "maxsize_large"
    files = sorted(root.glob("*.json"))
    total_rows = 0
    seqs = set()
    for f in files:
        with open(f) as fh:
            for line in fh:
                line = line.strip()
                if line:
                    obj = json.loads(line)
                    seqs.add(obj["seq"])
                    total_rows += 1

    print(f"total_rows: {total_rows}")
    print(f"all_seqs_present: {seqs == set(range(5000))}")
    print(f"no_duplicates: {total_rows == 5000}")


if __name__ == "__main__":
    main()
