# runner: python
"""Verify list-element evolution and that no rows were lost mid-stream."""

import os

# /// script
# dependencies = ["pyiceberg[pyarrow]"]
# ///

from pyiceberg.catalog.rest import RestCatalog


def main() -> None:
    catalog = RestCatalog("test", uri=os.environ["ICEBERG_REST_URI"])
    table = catalog.load_table("evolvens.events")
    print(table.schema())
    rows = sorted(table.scan().to_arrow().to_pylist(), key=lambda row: row["id"])
    print(f"rows: {len(rows)}")
    for row in rows:
        print(row["id"], row["items"], row["region"])


if __name__ == "__main__":
    main()
