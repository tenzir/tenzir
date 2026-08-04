# runner: python
"""Verify the conversion matrix via PyIceberg, the interop oracle.

Narrowed values survive at the boundaries and null beyond them, type
mismatches null whole columns, timestamps truncate to microseconds, and
durations land as nanosecond counts.
"""

import os

# /// script
# dependencies = ["pyiceberg[pyarrow]"]
# ///

from pyiceberg.catalog.rest import RestCatalog


def main() -> None:
    catalog = RestCatalog("test", uri=os.environ["ICEBERG_REST_URI"])
    table = catalog.load_table("convertns.matrix")
    rows = table.scan().to_arrow()
    for row in sorted(rows.to_pylist(), key=lambda row: row["k"]):
        print(
            row["k"],
            row["l"],
            row["dl"],
            row["i"],
            row["s"],
            row["n"],
            row["ts"],
            row["dur"],
            row["x"],
        )


if __name__ == "__main__":
    main()
