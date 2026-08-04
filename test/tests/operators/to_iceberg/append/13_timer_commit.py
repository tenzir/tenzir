# runner: python
"""Rows become visible through the commit timer while the stream stays open.

The `timeout` argument bounds both freshness and the crash-exposure
window, so the timer path must commit on its own: no rotation, no end of
input. The writer holds stdin open until the row is readable.
"""

import json
import os
import subprocess
import time

# /// script
# dependencies = ["pyiceberg[pyarrow]"]
# ///

from pyiceberg.catalog.rest import RestCatalog

TABLE = "testns.timer"


def main() -> None:
    catalog_uri = os.environ["ICEBERG_REST_URI"]
    pipeline = f"""
from_stdin {{
  read_ndjson
}}
to_iceberg "{TABLE}", catalog="{catalog_uri}", mode="create_append", timeout=2s
"""
    writer = subprocess.Popen(
        ["tenzir", pipeline],
        stdin=subprocess.PIPE,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )
    assert writer.stdin is not None
    writer.stdin.write(json.dumps({"id": 1}) + "\n")
    writer.stdin.flush()
    # Generous headroom: an ASan-instrumented debug build on a loaded
    # machine has been measured taking close to 30s for a first commit.
    catalog = RestCatalog("test", uri=catalog_uri)
    visible = False
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        try:
            if catalog.load_table(TABLE).scan().to_arrow().num_rows >= 1:
                visible = True
                break
        except Exception:
            pass
        time.sleep(0.5)
    print(f"visible before eof: {visible}")
    _, stderr = writer.communicate(timeout=120)
    print(f"writer exited with {writer.returncode}")
    if writer.returncode != 0:
        raise RuntimeError(stderr)
    rows = catalog.load_table(TABLE).scan().to_arrow().to_pylist()
    print(f"rows: {sorted(row['id'] for row in rows)}")


if __name__ == "__main__":
    main()
