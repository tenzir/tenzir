# runner: python
"""Verify the widen rotation: no row lost, pruned and full files coexist."""

import os
import urllib.parse

# /// script
# dependencies = ["pyiceberg[pyarrow]"]
# ///

import pyarrow.parquet as pq
from pyiceberg.catalog.rest import RestCatalog


def main() -> None:
    catalog = RestCatalog("test", uri=os.environ["ICEBERG_REST_URI"])
    table = catalog.load_table("prunens.events")
    rows = sorted(table.scan().to_arrow().to_pylist(), key=lambda row: row["id"])
    print(f"rows: {len(rows)}")
    widened = [row for row in rows if row["payload_b"] == "widen"]
    print(f"widened rows: {len(widened)}")
    print(widened[0]["id"], widened[0]["kind"], widened[0]["payload_a"])
    null_b = sum(1 for row in rows if row["payload_b"] is None)
    print(f"rows with null payload_b: {null_b}")
    # Report only which column sets coexist: the number of files per set
    # depends on rotation timing and is not deterministic.
    files = set()
    for task in table.scan().plan_files():
        path = urllib.parse.urlparse(task.file.file_path).path
        files.add(frozenset(pq.read_schema(path).names))
    for columns in sorted(files, key=sorted):
        print(f"files with columns: {sorted(columns)}")


if __name__ == "__main__":
    main()
