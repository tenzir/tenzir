# runner: python
"""After the step 04 truncating write the file should hold exactly one row."""

import json
import os
from pathlib import Path


def main() -> None:
    f = Path(os.environ["FILE_ROOT"]) / "out" / "append" / "log.json"
    rows = [json.loads(line) for line in f.read_text().splitlines() if line.strip()]
    print(f"rows: {len(rows)}")
    print(f"row: {rows[0]}")


if __name__ == "__main__":
    main()
