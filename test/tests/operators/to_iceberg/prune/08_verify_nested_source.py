# runner: python
"""Verify that pruning keeps the ancestors of nested partition sources."""

import os
import urllib.parse

# /// script
# dependencies = ["pyiceberg[pyarrow]"]
# ///

import pyarrow.parquet as pq
from pyiceberg.catalog.rest import RestCatalog


def main() -> None:
    catalog = RestCatalog("test", uri=os.environ["ICEBERG_REST_URI"])
    table = catalog.load_table("prunens.part_events")
    rows = sorted(table.scan().to_arrow().to_pylist(), key=lambda row: row["id"])
    print(f"rows: {len(rows)}")
    null_region = sum(1 for row in rows if row["meta"] is None)
    print(f"rows with null meta: {null_region}")
    # Every data file must carry the `meta` ancestor of the partition
    # source `meta.region`, even the ones whose rows are all-null in it.
    files_with_meta = 0
    files_total = 0
    for task in table.scan().plan_files():
        files_total += 1
        path = urllib.parse.urlparse(task.file.file_path).path
        names = set(pq.read_schema(path).names)
        if "meta" in {name.split(".")[0] for name in names}:
            files_with_meta += 1
    print(f"files with meta column: {files_with_meta} of {files_total}")


if __name__ == "__main__":
    main()
