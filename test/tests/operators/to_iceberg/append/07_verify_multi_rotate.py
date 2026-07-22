# runner: python
"""Verify that every rotated file landed as its own snapshot."""

import os

# /// script
# dependencies = ["pyiceberg[pyarrow]"]
# ///

from pyiceberg.catalog.rest import RestCatalog


def main() -> None:
    catalog = RestCatalog("test", uri=os.environ["ICEBERG_REST_URI"])
    table = catalog.load_table("testns.events")
    print(f"snapshots: {len(list(table.snapshots()))}")
    rows = table.scan().to_arrow().to_pylist()
    print(f"rows with id 5: {sum(1 for row in rows if row['id'] == 5)}")


if __name__ == "__main__":
    main()
