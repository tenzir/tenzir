# runner: python

import os
from pathlib import Path


def main() -> None:
    path = Path(os.environ["FILE_ROOT"]) / "out" / "write_all_blob.bin"
    data = path.read_bytes()
    print(data == b"\x00foo\x01bar\x00")


if __name__ == "__main__":
    main()
