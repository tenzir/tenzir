# Iceberg Plugin for Tenzir

Native Apache Iceberg output for Tenzir, built on
[apache/iceberg-cpp](https://github.com/apache/iceberg-cpp) (TNZ-774).

## Status: Phase 3 (continuous schema evolution)

The plugin provides the `to_iceberg` operator: it writes events into an
Apache Iceberg table through a REST catalog, creating the table from the
first arriving events when it does not exist (`mode` is one of
`create_append` (default), `create`, or `append`). Schema derivation maps
records to nested structs, ip/subnet/enumeration columns to strings,
timestamps to microsecond `timestamptz`, and durations and unsigned integers
to `long`. When the schema has a top-level `time` timestamp, created tables
register it as the default sort order and keep full column metrics for it
(all other columns store only counts, keeping manifests small for wide event
schemas).

Heterogeneous streams evolve the table continuously: fields the table does
not have yet — at any nesting depth, including list elements — commit a
metadata-only schema update before any data file carries them, so Parquet
files are only ever stamped with catalog-confirmed field IDs. Existing
columns are never modified; type conflicts null-fill with a warning, and
unrepresentable fields are dropped with a warning. Snapshot commits are
verified against the committed metadata (tagged via the `tenzir.commit-id`
summary property) and retried on top of concurrent updates, so racing
writers on the same table lose no data.

Layout:

- `builtins/to_iceberg.cpp`: the operator.
- `src/facade.cpp` + `include/tenzir/plugins/iceberg/facade.hpp`: the only
  code that talks to iceberg-cpp. The library is pre-1.0; keeping all usage
  behind this interface isolates API churn.
- `spike/spike.cpp`: throwaway Phase 0 exit-gate program. It creates a table
  through a REST catalog, writes one Parquet data file, and commits it as a
  FastAppend snapshot. Build with `-DTENZIR_ICEBERG_ENABLE_SPIKE=ON`.
- `aux/iceberg-cpp`: bundled dependency, pinned to a `main` commit
  (re-pinned to the voted 0.4.0 release before the operator goes stable).

See `to_iceberg-plan.md` on the `feat/to-iceberg-operator-plan` branch for
the full plan; hidden partitioning (Phase 4) and exactly-once delivery
(Phase 5) are next.

## Trying the spike

```sh
# Start a local REST catalog with a file-based warehouse shared with the host.
mkdir -p /private/tmp/iceberg_warehouse
docker run -d --name iceberg-rest -p 8181:8181 \
  -e CATALOG_WAREHOUSE=file:///tmp/iceberg_warehouse \
  -v /private/tmp/iceberg_warehouse:/tmp/iceberg_warehouse \
  apache/iceberg-rest-fixture:latest

# Create + append + commit.
iceberg-spike http://localhost:8181 "" spikens events

# Verify with PyIceberg.
uvx --with 'pyiceberg[pyarrow]' python plugins/iceberg/spike/verify_spike.py
```

## ASAN builds

Debug presets build Tenzir with AddressSanitizer, but the iceberg-cpp
libraries from the dev shell are uninstrumented. Mixing the two triggers
false-positive `container-overflow` reports from libc++'s container
annotations when iceberg-cpp parses table metadata. Run ASAN builds with:

```sh
export ASAN_OPTIONS=detect_container_overflow=0
```
