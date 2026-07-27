# runner: python
"""Create a foreign table whose `l` column is required.

The operator itself only ever creates optional columns, so the required
hard-error paths trigger exclusively against externally-authored tables.
"""

import os

# /// script
# dependencies = ["pyiceberg[pyarrow]"]
# ///

from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField


def main() -> None:
    catalog = RestCatalog("test", uri=os.environ["ICEBERG_REST_URI"])
    schema = Schema(
        NestedField(1, "k", LongType(), required=False),
        NestedField(2, "l", LongType(), required=True),
    )
    catalog.create_table("convertns.required", schema)
    print("created convertns.required")


if __name__ == "__main__":
    main()
