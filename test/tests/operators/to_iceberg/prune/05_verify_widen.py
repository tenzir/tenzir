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
    files = {}
    for task in table.scan().plan_files():
        path = urllib.parse.urlparse(task.file.file_path).path
        columns = frozenset(pq.read_schema(path).names)
        files[columns] = files.get(columns, 0) + 1
    for columns, count in sorted(files.items(), key=lambda item: sorted(item[0])):
        print(f"{count} file(s) with columns: {sorted(columns)}")


if __name__ == "__main__":
    main()
