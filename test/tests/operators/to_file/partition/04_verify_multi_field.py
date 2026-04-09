# runner: python
"""Verify multi-field hive partitioning creates correct directory structure."""

import os
from pathlib import Path


def main() -> None:
    root = Path(os.environ["FILE_ROOT"]) / "out" / "multi"
    # Walk the partition tree two levels deep and count files.
    partitions = []
    for region_dir in sorted(root.iterdir()):
        if not region_dir.is_dir():
            continue
        for tier_dir in sorted(region_dir.iterdir()):
            if not tier_dir.is_dir():
                continue
            n_files = len(list(tier_dir.glob("*.json")))
            count = 0
            for f in tier_dir.glob("*.json"):
                with open(f) as fh:
                    count += sum(1 for line in fh if line.strip())
            partitions.append(
                f"{region_dir.name}/{tier_dir.name}: files={n_files} rows={count}"
            )

    for p in partitions:
        print(p)


if __name__ == "__main__":
    main()
