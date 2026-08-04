# runner: python
"""Verify row-level cast fallback via PyIceberg, the interop oracle.

The overflowing value reads as null while its neighbor from the same batch
keeps its value.
"""

import os

# /// script
# dependencies = ["pyiceberg[pyarrow]"]
# ///

from pyiceberg.catalog.rest import RestCatalog


def main() -> None:
    catalog = RestCatalog("test", uri=os.environ["ICEBERG_REST_URI"])
    table = catalog.load_table("testns.events")
    rows = table.scan(row_filter="ts >= '2026-01-01T00:00:07+00:00'").to_arrow()
    for row in sorted(rows.to_pylist(), key=lambda row: row["message"]):
        print(row["id"], row["message"])


if __name__ == "__main__":
    main()
