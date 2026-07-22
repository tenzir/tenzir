# runner: python
"""Losing the create-table race must still validate `partition_by`.

A writer in `mode="create_append"` finds no table at start. Before its
first input arrives, another writer creates the same table with a
different partition spec. The lost race falls back to loading the
winner's table, which must fail the same `partition_by` compatibility
check that guards tables existing at start -- not silently append with
the winner's spec.
"""

import json
import os
import subprocess
import threading

# /// script
# dependencies = ["pyiceberg[pyarrow]"]
# ///

from pyiceberg.catalog.rest import RestCatalog

TABLE = "racens.spec"
START_MARKER = "does not exist; creating it from the first input schema"


def main() -> None:
    catalog_uri = os.environ["ICEBERG_REST_URI"]
    pipeline = f"""
from_stdin {{
  read_ndjson
}}
to_iceberg "{TABLE}", catalog="{catalog_uri}", mode="create_append",
  partition_by=[id]
"""
    writer = subprocess.Popen(
        ["tenzir", "--console-verbosity=debug", pipeline],
        stdin=subprocess.PIPE,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )
    assert writer.stdin is not None
    assert writer.stderr is not None
    # Wait until the writer's start() has concluded that the table does not
    # exist; only then does the external creation below race the create in
    # ensure_table instead of being seen at start.
    stderr_lines: list[str] = []
    for line in writer.stderr:
        stderr_lines.append(line)
        if START_MARKER in line:
            break
    else:
        raise RuntimeError("".join(stderr_lines))
    drain = threading.Thread(
        target=lambda: stderr_lines.extend(writer.stderr),  # type: ignore[arg-type]
    )
    drain.start()
    # Another writer wins the race with a different partition spec.
    subprocess.run(
        [
            "tenzir",
            f'from {{id: 1, value: "external"}}\n'
            f'to_iceberg "{TABLE}", catalog="{catalog_uri}", '
            f"partition_by=[value]",
        ],
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    writer.stdin.write(json.dumps({"id": 2, "value": "raced"}) + "\n")
    writer.stdin.close()
    returncode = writer.wait(timeout=30)
    drain.join(timeout=10)
    # The mismatch diagnostic makes the writer fail; the raced row must not
    # land in the winner's table.
    print(f"writer exited with {returncode}")
    catalog = RestCatalog("test", uri=os.environ["ICEBERG_REST_URI"])
    table = catalog.load_table(TABLE)
    rows = sorted(
        (row["id"], row["value"]) for row in table.scan().to_arrow().to_pylist()
    )
    print(f"rows: {rows}")


if __name__ == "__main__":
    main()
