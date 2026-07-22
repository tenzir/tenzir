# runner: python
"""Verify the appended rows via PyIceberg, the interop oracle."""

import os

# /// script
# dependencies = ["pyiceberg[pyarrow]"]
# ///

from pyiceberg.catalog.rest import RestCatalog


def main() -> None:
    catalog = RestCatalog("test", uri=os.environ["ICEBERG_REST_URI"])
    table = catalog.load_table("testns.events")
    print(table.schema())
    snapshots = list(table.snapshots())
    print(f"snapshots: {len(snapshots)}")
    for snapshot in snapshots:
        print(f"operation: {snapshot.summary.operation.value}")
    rows = sorted(table.scan().to_arrow().to_pylist(), key=lambda row: row["id"])
    for row in rows:
        print(row["id"], row["message"], row["ts"].isoformat())


if __name__ == "__main__":
    main()
