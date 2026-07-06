# runner: python
"""Verify that the second pipeline run appended a snapshot."""

import os

# /// script
# dependencies = ["pyiceberg[pyarrow]"]
# ///

from pyiceberg.catalog.rest import RestCatalog


def main() -> None:
    catalog = RestCatalog("test", uri=os.environ["ICEBERG_REST_URI"])
    table = catalog.load_table("createns.events")
    snapshots = list(table.snapshots())
    print(f"snapshots: {len(snapshots)}")
    rows = sorted(table.scan().to_arrow().to_pylist(), key=lambda row: row["id"])
    print(f"rows: {len(rows)}")
    print(rows[-1])


if __name__ == "__main__":
    main()
