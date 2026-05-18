# runner: python
"""All three rows from steps 01 and 02 should be present in append order."""

import json
import os
from pathlib import Path


def main() -> None:
    f = Path(os.environ["FILE_ROOT"]) / "out" / "append" / "log.json"
    assert f.exists(), f"output file missing: {f}"
    rows = [json.loads(line) for line in f.read_text().splitlines() if line.strip()]
    print(f"rows: {len(rows)}")
    print(f"seqs: {[r['seq'] for r in rows]}")
    print(f"msgs: {[r['msg'] for r in rows]}")


if __name__ == "__main__":
    main()
