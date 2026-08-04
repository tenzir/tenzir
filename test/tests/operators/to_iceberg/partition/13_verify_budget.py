# runner: python
"""Verify the budget-constrained append via PyIceberg, the interop oracle.

Early buffer closes ride along with the final commit, so the second append
adds exactly one snapshot and no row is lost.
"""

import os

# /// script
# dependencies = ["pyiceberg[pyarrow]"]
# ///

from pyiceberg.catalog.rest import RestCatalog


def main() -> None:
    catalog = RestCatalog("test", uri=os.environ["ICEBERG_REST_URI"])
    table = catalog.load_table("partns.many")
    print(f"snapshots: {len(list(table.snapshots()))}")
    print(f"rows: {table.scan().to_arrow().num_rows}")


if __name__ == "__main__":
    main()
