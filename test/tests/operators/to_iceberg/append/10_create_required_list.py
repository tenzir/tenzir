# runner: python
"""Create a table whose list elements carry a required struct child."""

import os

# /// script
# dependencies = ["pyiceberg[pyarrow]"]
# ///

from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError
from pyiceberg.schema import Schema
from pyiceberg.types import ListType, LongType, NestedField, StringType, StructType


def main() -> None:
    catalog = RestCatalog("test", uri=os.environ["ICEBERG_REST_URI"])
    try:
        catalog.create_namespace("testns")
    except NamespaceAlreadyExistsError:
        pass
    schema = Schema(
        NestedField(1, "id", LongType(), required=False),
        NestedField(
            2,
            "xs",
            ListType(
                element_id=3,
                element=StructType(
                    NestedField(4, "req", LongType(), required=True),
                    NestedField(5, "note", StringType(), required=False),
                ),
                element_required=False,
            ),
            required=False,
        ),
    )
    catalog.create_table("testns.required_list", schema)
    print("created testns.required_list")


if __name__ == "__main__":
    main()
