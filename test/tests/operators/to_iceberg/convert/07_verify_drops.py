# runner: python
"""Verify column drops never cost rows via PyIceberg, the interop oracle.

The table holds exactly the one representable column, and both events
survive as rows: one with its value, one as all nulls.
"""

import os

# /// script
# dependencies = ["pyiceberg[pyarrow]"]
# ///

from pyiceberg.catalog.rest import RestCatalog


def main() -> None:
    catalog = RestCatalog("test", uri=os.environ["ICEBERG_REST_URI"])
    table = catalog.load_table("convertns.drops")
    print(f"columns: {sorted(field.name for field in table.schema().fields)}")
    rows = table.scan().to_arrow().to_pylist()
    print(f"rows: {sorted((row['k'] is None, row['k']) for row in rows)}")


if __name__ == "__main__":
    main()
