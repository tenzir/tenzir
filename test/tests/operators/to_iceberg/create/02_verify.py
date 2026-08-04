# runner: python
"""Verify the created table via PyIceberg, the interop oracle.

Checks the derived schema (nested struct, string-mapped ip/subnet,
microsecond timestamptz), the registered sort order on `time`, the
conservative metrics properties, and the row values.
"""

import os

# /// script
# dependencies = ["pyiceberg[pyarrow]"]
# ///

from pyiceberg.catalog.rest import RestCatalog


def main() -> None:
    catalog = RestCatalog("test", uri=os.environ["ICEBERG_REST_URI"])
    table = catalog.load_table("createns.events")
    print(f"location: {table.location() == os.environ['ICEBERG_TABLE_LOCATION']}")
    print(table.schema())
    print(f"sort_order: {table.sort_order()}")
    for key in sorted(table.properties):
        if key.startswith("write.metadata."):
            print(f"{key}: {table.properties[key]}")
    rows = sorted(table.scan().to_arrow().to_pylist(), key=lambda row: row["id"])
    for row in rows:
        print(
            row["id"],
            row["time"].isoformat(),
            row["src_ip"],
            row["net"],
            row["dur"],
            row["meta"],
        )


if __name__ == "__main__":
    main()
