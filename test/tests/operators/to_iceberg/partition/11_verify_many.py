# runner: python
"""Verify the high-cardinality append via PyIceberg, the interop oracle.

All partitions land in a single snapshot, one per distinct id.
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
    print(f"partitions: {len(table.inspect.partitions())}")
    print(f"rows: {table.scan().to_arrow().num_rows}")


if __name__ == "__main__":
    main()
