# runner: python
"""Create a foreign table with narrow numeric columns.

Tenzir never derives `int` or `float`, so type widening only triggers on
tables created by other engines.
"""

import os

# /// script
# dependencies = ["pyiceberg[pyarrow]"]
# ///

from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import FloatType, IntegerType, NestedField, StringType


def main() -> None:
    catalog = RestCatalog("test", uri=os.environ["ICEBERG_REST_URI"])
    schema = Schema(
        NestedField(1, "id", IntegerType(), required=False),
        NestedField(2, "score", FloatType(), required=False),
        NestedField(3, "message", StringType(), required=False),
    )
    catalog.create_table("evolvens.narrow", schema)
    print("created evolvens.narrow")


if __name__ == "__main__":
    main()
