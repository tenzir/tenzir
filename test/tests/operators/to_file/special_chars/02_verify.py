# runner: python
"""Verify that a file with a space in its path was written correctly."""

import json
import os
from pathlib import Path


def main() -> None:
    target = Path(os.environ["FILE_ROOT"]) / "out dir" / "spaced file.json"
    assert target.exists(), f"expected file not found: {target}"
    lines = [line for line in target.read_text().splitlines() if line.strip()]
    assert len(lines) == 10, f"expected 10 lines, got {len(lines)}"
    for i, line in enumerate(lines):
        obj = json.loads(line)
        assert obj == {"msg": "hello", "x": i}, f"bad event at line {i}: {obj}"
    print("ok")


if __name__ == "__main__":
    main()
