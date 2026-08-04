# runner: python
"""Two concurrent pipelines evolve and append to the same table.

Each writer brings its own nested record and commits several small files,
racing table creation, schema updates, and snapshot commits against the
other. Field IDs depend on which writer wins the creation race, so only
names and counts are asserted.
"""

import os
import subprocess
from collections import Counter

# /// script
# dependencies = ["pyiceberg[pyarrow]"]
# ///

from pyiceberg.catalog.rest import RestCatalog

PIPELINE = """
from {{id: {id}, time: 2026-01-01T00:00:0{id}Z, {field}: {{value: {id}}}}}
repeat 4
to_iceberg "evolvens.multi", catalog="{catalog}", max_size=1
"""


def main() -> None:
    catalog_uri = os.environ["ICEBERG_REST_URI"]
    writers = [
        subprocess.Popen(
            [
                "tenzir",
                PIPELINE.format(id=id, field=field, catalog=catalog_uri),
            ],
        )
        for id, field in ((1, "alpha"), (2, "beta"))
    ]
    for writer in writers:
        print(f"writer exited with {writer.wait()}")
    catalog = RestCatalog("test", uri=catalog_uri)
    table = catalog.load_table("evolvens.multi")
    fields = sorted(field.name for field in table.schema().fields)
    print(f"columns: {fields}")
    rows = table.scan().to_arrow().to_pylist()
    print(f"rows: {sorted(Counter(row['id'] for row in rows).items())}")
    print(f"snapshots: {len(list(table.snapshots()))}")


if __name__ == "__main__":
    main()
