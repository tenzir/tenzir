# runner: python
"""Verify null partition keys via PyIceberg, the interop oracle.

Both rows are readable: the fully-keyed row and the row whose partition
sources are all null.
"""

import os

# /// script
# dependencies = ["pyiceberg[pyarrow]"]
# ///

from pyiceberg.catalog.rest import RestCatalog


def main() -> None:
    catalog = RestCatalog("test", uri=os.environ["ICEBERG_REST_URI"])
    table = catalog.load_table("partns.nullkeys")
    rows = table.scan().to_arrow().to_pylist()
    print(f"rows: {len(rows)}")
    for row in sorted(rows, key=lambda row: row["msg"]):
        print(row["msg"], row["id"], row["region"], row["ts"])


if __name__ == "__main__":
    main()
