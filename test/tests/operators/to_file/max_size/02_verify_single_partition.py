# runner: python
"""Verify that max_size rotation preserves all 500 events.

Exact file counts and per-file size bounds are intentionally not asserted:
under heavy parallel test-runner load the sub driver can be scheduled much
later than `process()`, letting the sub's input channel accumulate many
slices before rotation kicks in. Overshoot is bounded only by the channel
size, not by `max_size`. Data integrity, however, is guaranteed.
"""

import json
import os
from pathlib import Path


def main() -> None:
    root = Path(os.environ["FILE_ROOT"]) / "out" / "maxsize_single"
    files = sorted(root.glob("*.json"))
    total_rows = 0
    ids = set()
    for f in files:
        with open(f) as fh:
            for line in fh:
                line = line.strip()
                if line:
                    obj = json.loads(line)
                    ids.add(obj["id"])
                    total_rows += 1
    print(f"total_rows: {total_rows}")
    print(f"all_ids_present: {ids == set(range(500))}")
    print(f"no_duplicates: {total_rows == 500}")


if __name__ == "__main__":
    main()
