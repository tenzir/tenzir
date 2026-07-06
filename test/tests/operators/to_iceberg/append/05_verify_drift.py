# runner: python
"""Verify drifted rows: dropped extra column, null-filled message."""

import os

# /// script
# dependencies = ["pyiceberg[pyarrow]"]
# ///

from pyiceberg.catalog.rest import RestCatalog


def main() -> None:
    catalog = RestCatalog("test", uri=os.environ["ICEBERG_REST_URI"])
    table = catalog.load_table("testns.events")
    print(f"snapshots: {len(list(table.snapshots()))}")
    print(f"columns: {[field.name for field in table.schema().fields]}")
    rows = sorted(table.scan().to_arrow().to_pylist(), key=lambda row: row["id"])
    for row in rows:
        message = "null" if row["message"] is None else row["message"]
        print(row["id"], message, row["ts"].isoformat())


if __name__ == "__main__":
    main()
