# runner: python
"""Verify that budget-evicted files commit before the input reaches EOF."""

import os
import subprocess
import time

# /// script
# dependencies = ["pyiceberg[pyarrow]"]
# ///

from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.exceptions import NoSuchTableError

PIPELINE = """
from_stdin {
  read_ndjson
}
to_iceberg "partns.live", catalog=env("ICEBERG_REST_URI"),
  buffer_size=1, timeout=200ms
"""


def main() -> None:
    writer = subprocess.Popen(
        ["tenzir", PIPELINE],
        stdin=subprocess.PIPE,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )
    assert writer.stdin is not None
    writer.stdin.write('{"id": 1, "msg": "x"}\n')
    writer.stdin.flush()

    catalog = RestCatalog("test", uri=os.environ["ICEBERG_REST_URI"])
    # The deadline covers process startup, which alone takes >10 seconds
    # under ASan-instrumented debug builds; success breaks the loop early.
    deadline = time.monotonic() + 60
    rows = 0
    while time.monotonic() < deadline:
        if writer.poll() is not None:
            break
        try:
            table = catalog.load_table("partns.live")
            if list(table.snapshots()):
                rows = table.scan().to_arrow().num_rows
        except NoSuchTableError:
            pass
        if rows == 1:
            break
        time.sleep(0.1)

    print(f"rows before EOF: {rows}")
    writer.stdin.close()
    returncode = writer.wait(timeout=30)
    print(f"writer exited with {returncode}")
    if rows != 1 or returncode != 0:
        assert writer.stderr is not None
        raise RuntimeError(writer.stderr.read())


if __name__ == "__main__":
    main()
