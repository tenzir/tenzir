# Iceberg Plugin for Tenzir

Native Apache Iceberg output for Tenzir, built on
[apache/iceberg-cpp](https://github.com/apache/iceberg-cpp) (TNZ-774).

## Status: Phase 0 (spike + skeleton)

The plugin currently contains:

- `src/facade.cpp` + `include/tenzir/plugins/iceberg/facade.hpp`: the only
  code that talks to iceberg-cpp. The library is pre-1.0; keeping all usage
  behind this interface isolates API churn.
- `spike/spike.cpp`: throwaway Phase 0 exit-gate program. It creates a table
  through a REST catalog, writes one Parquet data file, and commits it as a
  FastAppend snapshot. Build with `-DTENZIR_ICEBERG_ENABLE_SPIKE=ON`.
- `aux/iceberg-cpp`: bundled dependency, pinned to a `main` commit
  (re-pinned to the voted 0.4.0 release before the operator goes stable).

The `to_iceberg` operator lands in Phase 1. See `to_iceberg-plan.md` on the
`feat/to-iceberg-operator-plan` branch for the full plan.

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
