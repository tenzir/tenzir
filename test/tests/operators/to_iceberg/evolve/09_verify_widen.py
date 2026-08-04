# runner: python
"""Verify type widening via PyIceberg, the interop oracle.

The columns keep their field IDs but promote to long and double, and the
appended values survive intact.
"""

import os

# /// script
# dependencies = ["pyiceberg[pyarrow]"]
# ///

from pyiceberg.catalog.rest import RestCatalog


def main() -> None:
    catalog = RestCatalog("test", uri=os.environ["ICEBERG_REST_URI"])
    table = catalog.load_table("evolvens.narrow")
    print(table.schema())
    for row in table.scan().to_arrow().to_pylist():
        print(row["id"], row["score"], row["message"])


if __name__ == "__main__":
    main()
