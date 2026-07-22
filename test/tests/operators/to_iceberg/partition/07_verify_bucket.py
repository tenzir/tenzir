# runner: python
"""Cross-check bucket and truncate partition values against PyIceberg's own
transform implementations (the spec's Murmur3 bucket hash in particular),
and verify the nested identity source."""

import os

# /// script
# dependencies = ["pyiceberg[pyarrow]"]
# ///

from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.transforms import BucketTransform, TruncateTransform
from pyiceberg.types import LongType, StringType


def main() -> None:
    catalog = RestCatalog("test", uri=os.environ["ICEBERG_REST_URI"])
    table = catalog.load_table("partns.hashed")
    print(table.spec())
    bucket = BucketTransform(num_buckets=4).transform(LongType())
    truncate = TruncateTransform(width=2).transform(StringType())
    rows = sorted(table.scan().to_arrow().to_pylist(), key=lambda row: row["id"])
    expected = {
        (bucket(row["id"]), truncate(row["name"]), row["meta"]["region"])
        for row in rows
    }
    actual = {
        (
            row["partition"]["id_bucket_4"],
            row["partition"]["name_trunc_2"],
            row["partition"]["meta.region"],
        )
        for row in table.inspect.partitions().to_pylist()
    }
    print(f"partition tuples match PyIceberg transforms: {expected == actual}")
    for tuple_ in sorted(actual):
        print(*tuple_)
    for row in rows:
        print(row["id"], row["name"], row["meta"]["region"])


if __name__ == "__main__":
    main()
