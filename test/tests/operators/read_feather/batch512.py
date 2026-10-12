# runner: python

"""Check that the reader hands on the record batches it finds, unchanged.

The encoder runs in its own process, and without Nova, so that the batch sizes
the reader observes are the ones the writer chose rather than the ones a shared
pipeline happens to produce. There is no way to express that in TQL, and the
encoded file is too large to check in.
"""

import json
import os
import shlex
import subprocess


def run(pipeline: str, *, nova: bool, data: bytes | None = None):
    command = [*shlex.split(os.environ["TENZIR_BINARY"]), "--bare-mode"]
    command.append(f"--nova={str(nova).lower()}")
    result = subprocess.run(
        [*command, pipeline], input=data, capture_output=True, timeout=60
    )
    assert result.returncode == 0, (pipeline, result.stderr.decode())
    assert not result.stderr, result.stderr.decode()
    return result.stdout


encoded = run(
    'from_file f"{env("TENZIR_INPUTS")}/zeek/conn.log.gz" {\n'
    "  decompress_gzip\n"
    "  read_zeek_tsv\n"
    "}\n"
    "batch 512\n"
    "write_feather",
    nova=False,
)
decoded = run(
    "load_stdin\nread_feather\nmeasure by_schema=false\ndrop timestamp\n"
    "write_json compact=true",
    nova=True,
    data=encoded,
)
rows = [json.loads(line) for line in decoded.splitlines()]
# `measure by_schema=false` reports one metric per batch, regardless of how many
# schemas its events have.
expected_sizes = [512] * 16 + [270]
sizes = [row["events"] for row in rows]
assert sizes == expected_sizes, (sizes, rows)
for size in sizes:
    print(f"{{\n  events: {size},\n}}")
