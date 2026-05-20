# runner: python
"""Each region's file should hold both writes worth of rows."""

import json
import os
from pathlib import Path


def main() -> None:
    root = Path(os.environ["FILE_ROOT"]) / "out" / "append" / "by_region"
    for region, expected in [("us", ["a", "b", "d", "e"]), ("eu", ["c", "f"])]:
        f = root / f"region={region}" / "data.json"
        rows = [json.loads(line) for line in f.read_text().splitlines() if line.strip()]
        vals = [r["val"] for r in rows]
        print(f"{region}: {vals} ok={vals == expected}")


if __name__ == "__main__":
    main()
