# runner: python
"""Verify that appending without `partition_by` fans out per the table's
own partition spec: the new day opens a new partition, and the second
writer run adds a second file to an existing partition."""

import os

# /// script
# dependencies = ["pyiceberg[pyarrow]"]
# ///

from pyiceberg.catalog.rest import RestCatalog


def main() -> None:
    catalog = RestCatalog("test", uri=os.environ["ICEBERG_REST_URI"])
    table = catalog.load_table("partns.events")
    partitions = sorted(
        (
            (
                row["partition"]["class_uid"],
                str(row["partition"]["time_day"]),
                row["record_count"],
                row["file_count"],
            )
            for row in table.inspect.partitions().to_pylist()
        ),
    )
    for class_uid, day, records, files in partitions:
        print(
            f"partition class_uid={class_uid} time_day={day}: "
            f"{records} rows in {files} file(s)"
        )
    print(f"total rows: {len(table.scan().to_arrow())}")


if __name__ == "__main__":
    main()
