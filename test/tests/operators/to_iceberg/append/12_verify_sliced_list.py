# runner: python
"""Verify the sliced write landed exactly the referenced row."""

import os

# /// script
# dependencies = ["pyiceberg[pyarrow]"]
# ///

from pyiceberg.catalog.rest import RestCatalog


def main() -> None:
    catalog = RestCatalog("test", uri=os.environ["ICEBERG_REST_URI"])
    table = catalog.load_table("testns.required_list")
    rows = table.scan().to_arrow().to_pylist()
    print(f"rows: {len(rows)}")
    for row in rows:
        print(row["id"], row["xs"])


if __name__ == "__main__":
    main()
