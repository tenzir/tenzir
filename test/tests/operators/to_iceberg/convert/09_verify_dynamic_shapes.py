# runner: python
"""Verify the dynamic shape lifecycle via PyIceberg, the interop oracle.

All nine rows survive. Columns added mid-stream read back for earlier
rows as nulls, null-typed arrivals read as nulls, and empty lists stay
empty lists rather than nulls.
"""

import os

# /// script
# dependencies = ["pyiceberg[pyarrow]"]
# ///

from pyiceberg.catalog.rest import RestCatalog


def main() -> None:
    catalog = RestCatalog("test", uri=os.environ["ICEBERG_REST_URI"])
    table = catalog.load_table("convertns.shapes")
    print(f"columns: {sorted(field.name for field in table.schema().fields)}")
    rows = table.scan().to_arrow().to_pylist()
    print(f"rows: {len(rows)}")
    for row in sorted(rows, key=lambda row: row["k"]):
        print(
            row["k"],
            row["msg"],
            "|",
            row["labels"],
            "|",
            row["unmapped"],
            "|",
            row["obs"],
        )


if __name__ == "__main__":
    main()
