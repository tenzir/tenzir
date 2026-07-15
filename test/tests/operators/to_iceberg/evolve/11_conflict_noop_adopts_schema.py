# runner: python
"""A no-op schema evolution must still adopt the table's current schema.

A running writer creates the table, then another writer evolves the
schema with a column the running writer is about to add itself. The
running writer's own evolution becomes a no-op -- either because its
schema update conflicts and the retry against the reloaded table finds
the column already present, or because the table handle refreshes and
the diff is empty from the start. Either way the writer must adopt the
table's current schema; otherwise projection against the stale cached
schema silently drops the new column from its own rows.
"""

import json
import os
import subprocess
import time

# /// script
# dependencies = ["pyiceberg[pyarrow]"]
# ///

from pyiceberg.catalog.rest import RestCatalog

TABLE = "evolvens.conflict_noop"


def main() -> None:
    catalog_uri = os.environ["ICEBERG_REST_URI"]
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
    writer.stdin.write(json.dumps({"id": 1}) + "\n")
    writer.stdin.flush()
    # Wait for the writer to create the table and commit the first row, so
    # that its in-memory table handle predates the external evolution below.
    catalog = RestCatalog("test", uri=catalog_uri)
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        try:
            if catalog.load_table(TABLE).scan().to_arrow().num_rows >= 1:
                break
        except Exception:
            pass
        time.sleep(0.1)
    else:
        raise RuntimeError("first row never committed")
    # Another writer adds the `extra` column behind the running writer's back.
    subprocess.run(
        [
            "tenzir",
            f"from {{id: 2, extra: 42}}\n"
            f'to_iceberg "{TABLE}", catalog="{catalog_uri}", mode="append"',
        ],
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    # The running writer now needs the same column: its schema update
    # conflicts, and the retry against the reloaded table is a no-op.
    writer.stdin.write(json.dumps({"id": 3, "extra": 7}) + "\n")
    _, stderr = writer.communicate(timeout=30)
    print(f"writer exited with {writer.returncode}")
    if writer.returncode != 0:
        raise RuntimeError(stderr)
    table = catalog.load_table(TABLE)
    rows = sorted(
        (row["id"], row["extra"]) for row in table.scan().to_arrow().to_pylist()
    )
    print(f"rows: {rows}")
    print(f"columns: {sorted(field.name for field in table.schema().fields)}")


if __name__ == "__main__":
    main()
