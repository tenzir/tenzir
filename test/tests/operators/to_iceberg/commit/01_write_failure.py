# runner: python
"""A failed data-file write stops the pipeline without corrupting the table.

After the first commit, the table's data directory turns read-only. The
next file the operator tries to write fails, the pipeline errors out,
and everything committed before stays fully readable: no partial file
ever enters the table metadata.
"""

import json
import os
import subprocess
import time
from pathlib import Path

# /// script
# dependencies = ["pyiceberg[pyarrow]"]
# ///

from pyiceberg.catalog.rest import RestCatalog

TABLE = "commitns.wfail"


def main() -> None:
    catalog_uri = os.environ["ICEBERG_REST_URI"]
    warehouse = Path(os.environ["ICEBERG_WAREHOUSE_DIR"])
    pipeline = f"""
from_stdin {{
  read_ndjson
}}
to_iceberg "{TABLE}", catalog="{catalog_uri}", mode="create_append", max_size=1
"""
    writer = subprocess.Popen(
        ["tenzir", pipeline],
        stdin=subprocess.PIPE,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )
    assert writer.stdin is not None
    catalog = RestCatalog("test", uri=catalog_uri)

    def rows() -> list[int]:
        try:
            scanned = catalog.load_table(TABLE).scan().to_arrow().to_pylist()
        except Exception:
            return []
        return sorted(row["id"] for row in scanned)

    writer.stdin.write(json.dumps({"id": 1}) + "\n")
    writer.stdin.flush()
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline and rows() != [1]:
        time.sleep(0.5)
    if rows() != [1]:
        raise RuntimeError("first row never committed")
    data_dir = warehouse / "commitns" / "wfail" / "data"
    print(f"data dir exists: {data_dir.is_dir()}")
    os.chmod(data_dir, 0o555)
    try:
        writer.stdin.write(json.dumps({"id": 2}) + "\n")
        try:
            writer.communicate(timeout=120)
        except subprocess.TimeoutExpired:
            writer.kill()
            raise
    finally:
        os.chmod(data_dir, 0o755)
    print(f"writer failed: {writer.returncode != 0}")
    print(f"rows: {rows()}")
    table = catalog.load_table(TABLE)
    print(f"snapshots: {len(list(table.snapshots()))}")


if __name__ == "__main__":
    main()
