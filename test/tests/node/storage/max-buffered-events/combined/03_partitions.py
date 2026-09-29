"""Run the partition-count query inside the node, then read its result."""

from __future__ import annotations

import json
import os
from pathlib import Path

tenzir = Executor.from_env(os.environ)
tenzir.binary = (*tenzir.binary, "--nova=true")
output = Path(os.environ["TENZIR_TMP_DIR"]) / "partitions.json"
result = tenzir.run(
    "pipeline_run {\n"
    "  partitions\n"
    '  where schema == "buffered"\n'
    "  summarize count=count()\n"
    f"  to_file {json.dumps(str(output))} {{ write_ndjson }}\n"
    "}\n"
)
assert result.returncode == 0, result.stderr.decode()
print(output.read_text().strip())
