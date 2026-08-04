# runner: python
"""Verify schema evolution on a partitioned table: the new column exists,
the partition spec is unchanged, and the event without a time landed in a
null partition."""

import os

# /// script
# dependencies = ["pyiceberg[pyarrow]"]
# ///

from pyiceberg.catalog.rest import RestCatalog


def main() -> None:
    catalog = RestCatalog("test", uri=os.environ["ICEBERG_REST_URI"])
    table = catalog.load_table("partns.events")
    print(table.schema())
    print(table.spec())
    partitions = sorted(
        (
            (
                str(row["partition"]["class_uid"]),
                str(row["partition"]["time_day"]),
                row["record_count"],
            )
            for row in table.inspect.partitions().to_pylist()
        ),
    )
    for class_uid, day, records in partitions:
        print(f"partition class_uid={class_uid} time_day={day}: {records} rows")
    rows = sorted(table.scan().to_arrow().to_pylist(), key=lambda row: row["msg"])
    for row in rows:
        print(
            row["msg"],
            row["time"].isoformat() if row["time"] is not None else None,
            row["severity"],
        )


if __name__ == "__main__":
    main()
