# runner: python
"""Verify schema evolution via PyIceberg, the interop oracle.

The original columns keep their field IDs, new columns get fresh
catalog-assigned IDs, and rows written before the evolution read as null
for the added columns.
"""

import os

# /// script
# dependencies = ["pyiceberg[pyarrow]"]
# ///

from pyiceberg.catalog.rest import RestCatalog


def main() -> None:
    catalog = RestCatalog("test", uri=os.environ["ICEBERG_REST_URI"])
    table = catalog.load_table("evolvens.events")
    print(table.schema())
    print(f"snapshots: {len(list(table.snapshots()))}")
    rows = sorted(table.scan().to_arrow().to_pylist(), key=lambda row: row["id"])
    for row in rows:
        print(row["id"], row["severity"], row["meta"], row["net"])


if __name__ == "__main__":
    main()
