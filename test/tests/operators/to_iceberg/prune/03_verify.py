# runner: python
"""Verify that pruned files omit all-null columns yet read back complete."""

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
    for row in (rows[0], rows[1], rows[2], rows[-1]):
        print(row["id"], row["kind"], row["payload_a"], row["payload_b"])
    # The narrow append never carried payload_b, so its data file must not
    # contain the column at all; the seed file carries both payloads.
    tasks = table.scan().plan_files()
    files = {}
    for task in tasks:
        path = urllib.parse.urlparse(task.file.file_path).path
        columns = frozenset(pq.read_schema(path).names)
        files[frozenset(columns)] = files.get(frozenset(columns), 0) + 1
    for columns, count in sorted(files.items(), key=lambda item: sorted(item[0])):
        print(f"{count} file(s) with columns: {sorted(columns)}")


if __name__ == "__main__":
    main()
