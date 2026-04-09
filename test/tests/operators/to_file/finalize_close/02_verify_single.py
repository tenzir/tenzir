# runner: python
"""If the pipeline completed without hanging, the partition's stream was
closed on shutdown. Verify the file exists and has the expected content."""

import json
import os
from pathlib import Path


def main() -> None:
    f = Path(os.environ["FILE_ROOT"]) / "out" / "finalize" / "single.json"
    assert f.exists(), f"output file missing: {f}"
    rows = [json.loads(line) for line in f.read_text().splitlines() if line.strip()]
    print(f"rows: {len(rows)}")
    print(f"content_ok: {rows == [{'x': 1}]}")


if __name__ == "__main__":
    main()
