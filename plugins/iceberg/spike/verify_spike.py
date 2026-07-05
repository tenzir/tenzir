"""Phase 0 exit gate: PyIceberg must read the table the spike wrote."""

import sys

from pyiceberg.catalog.rest import RestCatalog

catalog = RestCatalog("spike-verify", uri="http://localhost:8181")
table = catalog.load_table("spikens.events2")
print("schema:", table.schema())
snapshots = list(table.snapshots())
print("snapshots:", [(s.snapshot_id, s.summary.operation.value) for s in snapshots])
assert len(snapshots) == 1, f"expected 1 snapshot, got {len(snapshots)}"
files = list(table.inspect.files().to_pylist())
print("files:", [(f["file_path"], f["record_count"]) for f in files])
data = table.scan().to_arrow()
print("rows:", data.to_pylist())
assert data.num_rows == 3, f"expected 3 rows, got {data.num_rows}"
assert sorted(data.column("id").to_pylist()) == [0, 1, 2]
assert data.column("message").to_pylist()[0].startswith("event-")
print("OK: PyIceberg read back the spike table")
sys.exit(0)
