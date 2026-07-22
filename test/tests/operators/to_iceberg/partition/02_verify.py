# runner: python
"""Verify hidden partitioning via PyIceberg, the interop oracle.

Checks that the schema has no helper column, that the partition spec is
identity(class_uid) + day(time), that each partition tuple holds the
expected rows, and that partition pruning narrows a filtered scan to a
single data file.
"""

import os

# /// script
# dependencies = ["pyiceberg[pyarrow]"]
# ///

from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.expressions import And, EqualTo, GreaterThanOrEqual


def main() -> None:
    catalog = RestCatalog("test", uri=os.environ["ICEBERG_REST_URI"])
    table = catalog.load_table("partns.events")
    print(table.schema())
    print(table.spec())
    partitions = sorted(
        (
            (
                row["partition"]["class_uid"],
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
        print(row["class_uid"], row["time"].isoformat(), row["msg"])
    pruned = table.scan(
        row_filter=And(
            EqualTo("class_uid", 1001),
            GreaterThanOrEqual("time", "2026-01-02T00:00:00+00:00"),
        ),
    )
    print(f"pruned scan plans {len(list(pruned.plan_files()))} data file(s)")


if __name__ == "__main__":
    main()
