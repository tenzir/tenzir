# OCSF Iceberg write benchmarks

These benchmarks measure steady-state `to_iceberg` throughput for sparse OCSF
1.8 events. They keep schema normalization, catalog startup, table creation,
and schema evolution outside the timed region so the result measures the
operator's write path rather than test-data construction.

Run both benchmark groups from the repository root:

```sh
caffeinate -dimsu env \
  PATH="/opt/homebrew/opt/gnu-time/libexec/gnubin:$PATH" \
  uv run --project ~/code/tenzir/bench tenzir-bench run \
  --tenzir-bin build/xcode/release/bin/tenzir \
  --benchmark 'to_iceberg_ocsf_*'
```

The suite is opt-in and is not part of `bench/defaults.txt`. Keep macOS awake
while measuring: system sleep pauses the process but advances the wall clock
and can fire long-running writer timers.

## Workloads

The fixture normalizes the seed corpus with sparse `ocsf::cast` semantics and
caches typed BITZ batches before timing begins. The timed pipelines memory-map
the cache, repeat complete batches, and append them. This preserves schema
boundaries while removing NDJSON parsing, OCSF casting, and synthetic-field
construction from the measurement. The mixed discard control measures the
remaining BITZ read and repeat cost.

The seed corpus contains one representative event for each of 25 OCSF event
classes across five categories. It draws field shapes and source products from
the local Tenzir Library mappings for Microsoft Windows, Sysmon, Zeek, and
Suricata.

The homogeneous group writes 10,027,008 Process Activity or DNS Activity
events. Every run contains 153 schema-homogeneous batches of 65,536 events and
therefore exercises 153 Parquet row groups in one data file. It compares:

- A class-sized table seeded with only the selected event class.
- A unified table pre-seeded with all 25 OCSF event classes.

The mixed group writes 409,600 events as 100 batches of 4,096 events. It
contains four batches for each event class, so every class or partition gets
multiple row groups. It compares:

| Implementation | Tables | Partitioning |
|---|---:|---|
| `control` | 0 | Discards the prepared workload. |
| `unified-unpartitioned` | 1 | None. |
| `unified-partitioned` | 1 | `class_uid` and `day(time)`. |
| `category-tables` | 5 | One table per `category_uid`; each table uses `class_uid` and `day(time)`. |
| `class-tables` | 25 | One table per `class_uid`; each table uses `day(time)`. |

The Parquet writer creates a row group for every batch passed to it because
all benchmark batches are below its 1,048,576-row limit. Partitioned layouts
write 25 files with four row groups each; the unpartitioned unified layout
writes one file with 100 row groups.

The small NDJSON seed file expands before measurement. Its declared input
count is the logical event count so benchmark reports calculate events per
second correctly. Do not use the report's input-bytes rate for this suite.

## Validation

Every measured run writes an
`iceberg/<benchmark-id>/<implementation>/<phase>-<run>.json` sidecar in the
benchmark state directory. The fixture fails the run unless the append adds
the exact declared event count, leaves table schemas and partition
specifications unchanged, and produces the expected number of tables. The
sidecar includes the expected batch and row-group count, snapshot IDs,
data-file counts, file sizes, and warehouse byte deltas.

Use the median wall-clock time from the three measured runs. For the mixed
workloads, report both raw times and times after subtracting the discard
control. The control is a meaningful portion of the shorter runs, and omitting
it would overstate Iceberg overhead.

The suite does not measure cold schema evolution, compaction, or query
performance.
