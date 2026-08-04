# runner: python
"""Create the target table; `to_iceberg` only appends to existing tables."""

import os

# /// script
# dependencies = ["pyiceberg[pyarrow]"]
# ///

from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField, StringType, TimestamptzType


def main() -> None:
    catalog = RestCatalog("test", uri=os.environ["ICEBERG_REST_URI"])
    try:
        catalog.create_namespace("testns")
    except NamespaceAlreadyExistsError:
        pass
    schema = Schema(
        NestedField(1, "id", LongType(), required=False),
        NestedField(2, "message", StringType(), required=False),
        NestedField(3, "ts", TimestamptzType(), required=False),
    )
    catalog.create_table("testns.events", schema)
    print("created testns.events")


if __name__ == "__main__":
    main()
