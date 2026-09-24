# Catch-all write benchmark

`write.py` measures complete pipeline runtime for 100,000 events, with three
samples per case. It tests valid values, one incompatible value, and alternating
valid and incompatible values. Every sample checks stored row counts, dropped row
counts, absence of mapped values in the catch-all, and the sum of event IDs
through SQL. It also counts acknowledged INSERT queries through
`system.query_log`.

Run from the `engine` directory against a disposable ClickHouse instance using
the same environment as the ClickHouse integration fixture:

- `CLICKHOUSE_CONTAINER_RUNTIME` and `CLICKHOUSE_CONTAINER_ID` for SQL queries.
- `CLICKHOUSE_HOST`, `CLICKHOUSE_PORT`, and `CLICKHOUSE_PASSWORD` for insertion.
- `TENZIR_BINARY` for the binary under test.
- Optional `BENCH_BASELINE_BINARY` for a comparison binary.

```sh
python3 bench/manual/clickhouse_write/write.py
```

For builds with shared plugins, preserve both plugin versions and select each
one explicitly with `--plugins=/path/to/libtenzir-plugin-clickhouse.so` in the
binary command. Copying only the executable does not preserve the old writer.

The script creates a uniquely named Memory table and removes it afterward. It
writes results to `/tmp/clickhouse-write-benchmark.json`. Override the output
path, event count, or sample count with `BENCH_OUTPUT`, `BENCH_ROWS`, or
`BENCH_REPEATS`.

Results include process startup, JSON parsing, preparation, and insertion with
one worker. They do not measure sustained production throughput, MergeTree
merges, or allocation counts. Compare binaries on the same idle machine and
ClickHouse instance. The script requires query logging and permission to flush
system logs.

The invalid cases use `4294967296` for a `UInt32` column. Input is explicitly
converted to Tenzir's unsigned integer type before insertion. The writer warns
and drops affected events; it does not move the mapped value into the catch-all.
Results distinguish input events/s from stored events/s so rejected events do
not inflate the reported insertion throughput.

Use `BENCH_ROWS=100 BENCH_REPEATS=1` for a quick verification before a full run.
A comparison binary must implement the same drop behavior; older fallback-based
writers fail the SQL assertions.
