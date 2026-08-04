# runner: python
"""Create a foreign table covering the operator's conversion matrix.

Every conversion the operator performs targets one of these columns:
narrowing into `long` and `int`, type mismatches against `string` and
`long`, timestamp truncation, durations as nanosecond counts, and
null-typed input.
"""

import os

# /// script
# dependencies = ["pyiceberg[pyarrow]"]
# ///

from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError
from pyiceberg.schema import Schema
from pyiceberg.types import (
    IntegerType,
    LongType,
    NestedField,
    StringType,
    TimestamptzType,
)


def main() -> None:
    catalog = RestCatalog("test", uri=os.environ["ICEBERG_REST_URI"])
    try:
        catalog.create_namespace("convertns")
    except NamespaceAlreadyExistsError:
        pass
    try:
        catalog.drop_table("convertns.matrix")
    except NoSuchTableError:
        pass
    schema = Schema(
        NestedField(1, "k", LongType(), required=False),
        NestedField(2, "l", LongType(), required=False),
        NestedField(3, "dl", LongType(), required=False),
        NestedField(4, "i", IntegerType(), required=False),
        NestedField(5, "s", StringType(), required=False),
        NestedField(6, "n", LongType(), required=False),
        NestedField(7, "ts", TimestamptzType(), required=False),
        NestedField(8, "dur", LongType(), required=False),
        NestedField(9, "x", LongType(), required=False),
    )
    catalog.create_table("convertns.matrix", schema)
    print("created convertns.matrix")


if __name__ == "__main__":
    main()
