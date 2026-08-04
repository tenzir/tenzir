# runner: python
"""Verify the staged-file cap committed before the pipeline finished."""

import os

# /// script
# dependencies = ["pyiceberg[pyarrow]"]
# ///

from pyiceberg.catalog.rest import RestCatalog


def main() -> None:
    catalog = RestCatalog("test", uri=os.environ["ICEBERG_REST_URI"])
    table = catalog.load_table("partns.burst")
    snapshots = len(list(table.snapshots()))
    rows = table.scan().to_arrow().num_rows
    # One commit at the staged-file cap plus the finalize commit; a single
    # snapshot means everything waited for the end.
    print(f"multiple commits: {snapshots >= 2}")
    print(f"rows: {rows}")


if __name__ == "__main__":
    main()
