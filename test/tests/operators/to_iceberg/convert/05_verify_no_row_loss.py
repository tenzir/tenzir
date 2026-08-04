# runner: python
"""Verify that the malformed event lost a cell, never a row.

All 100 rows land across the many single-file commits; the type-flipped
event keeps its row with a null `l`, and every other row keeps its value.
"""

import os

# /// script
# dependencies = ["pyiceberg[pyarrow]"]
# ///

from pyiceberg.catalog.rest import RestCatalog


def main() -> None:
    catalog = RestCatalog("test", uri=os.environ["ICEBERG_REST_URI"])
    table = catalog.load_table("convertns.integrity")
    rows = table.scan().to_arrow().to_pylist()
    print(f"rows: {len(rows)}")
    print(f"distinct keys: {len({row['k'] for row in rows})}")
    print(f"null l: {[row['k'] for row in rows if row['l'] is None]}")
    print(f"intact: {sum(1 for row in rows if row['l'] == row['k'])}")
    print(f"multiple commits: {len(list(table.snapshots())) > 1}")


if __name__ == "__main__":
    main()
