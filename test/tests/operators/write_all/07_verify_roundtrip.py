# runner: python

import os
from pathlib import Path


def main() -> None:
    root = Path(os.environ["FILE_ROOT"]) / "out"
    original = root / "write_all_blob.bin"
    copy = root / "write_all_blob_copy.bin"
    print(original.read_bytes() == copy.read_bytes())


if __name__ == "__main__":
    main()
