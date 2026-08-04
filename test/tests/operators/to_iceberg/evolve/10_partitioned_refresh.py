# runner: python
"""Keep a partition open while another writer evolves the table schema."""

import json
import os
import subprocess
import time
from collections import Counter

# /// script
# dependencies = ["pyiceberg[pyarrow]"]
# ///

from pyiceberg.catalog.rest import RestCatalog

TABLE = "evolvens.partitioned_refresh"


def run_pipeline(pipeline: str) -> None:
    subprocess.run(
        ["tenzir", pipeline],
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )


def main() -> None:
    catalog_uri = os.environ["ICEBERG_REST_URI"]
    run_pipeline(
        f"""
from {{id: 0, value: "setup"}}
to_iceberg "{TABLE}", catalog="{catalog_uri}", partition_by=[id]
"""
    )

    pipeline = f"""
from_stdin {{
  read_ndjson
}}
to_iceberg "{TABLE}", catalog="{catalog_uri}", mode="append", max_size=1000
"""
    writer = subprocess.Popen(
        ["tenzir", pipeline],
        stdin=subprocess.PIPE,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )
    assert writer.stdin is not None
    writer.stdin.write(json.dumps({"id": 2, "value": "buffered"}) + "\n")
    writer.stdin.flush()
    # Best-effort scheduling: the event should buffer under the old schema
    # before the concurrent evolution below. There is nothing to poll --
    # buffered rows are deliberately invisible until a commit -- and the
    # refresh behavior under test holds in either interleaving.
    time.sleep(1)

    run_pipeline(
        f"""
from {{id: 3, value: "evolved", extra: 42}}
to_iceberg "{TABLE}", catalog="{catalog_uri}", mode="append"
"""
    )
    writer.stdin.write(json.dumps({"id": 1, "value": "x" * 4096}) + "\n")
    writer.stdin.flush()

    catalog = RestCatalog("test", uri=catalog_uri)
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        snapshots = list(catalog.load_table(TABLE).snapshots())
        if len(snapshots) >= 3:
            break
        time.sleep(0.1)

    writer.stdin.write(json.dumps({"id": 2, "value": "fresh"}) + "\n")
    writer.stdin.close()
    returncode = writer.wait(timeout=10)
    print(f"writer exited with {returncode}")
    if returncode != 0:
        assert writer.stderr is not None
        raise RuntimeError(writer.stderr.read())

    table = catalog.load_table(TABLE)
    rows = table.scan().to_arrow().to_pylist()
    counts = sorted(Counter(row["id"] for row in rows).items())
    print(f"rows: {counts}")
    print(f"columns: {sorted(field.name for field in table.schema().fields)}")


if __name__ == "__main__":
    main()
