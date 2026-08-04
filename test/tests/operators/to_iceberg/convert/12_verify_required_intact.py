# runner: python
"""Verify the rejected write left the required-column table untouched.

The failed pipeline must not have staged or committed any partial rows.
"""

import os

# /// script
# dependencies = ["pyiceberg[pyarrow]"]
# ///

from pyiceberg.catalog.rest import RestCatalog


def main() -> None:
    catalog = RestCatalog("test", uri=os.environ["ICEBERG_REST_URI"])
    table = catalog.load_table("convertns.required")
    rows = table.scan().to_arrow().to_pylist()
    print(f"rows: {len(rows)}")
    print(f"snapshots: {len(list(table.snapshots()))}")


if __name__ == "__main__":
    main()
