---
title: Read from and write to DuckDB databases
type: feature
authors:
  - mavam
created: 2026-09-22T16:23:58.138973Z
---

The new `from_duckdb` and `to_duckdb` operators read from and write to [DuckDB](https://duckdb.org) database files, with DuckDB embedded in Tenzir. No server is involved: point the operators at a file path, or at `:memory:` for a private in-memory database.

`from_duckdb` reads a table or runs arbitrary SQL:

```tql
from_duckdb "events.duckdb", table="alerts"
```

```tql
from_duckdb ":memory:", sql="SELECT * FROM 'flows.parquet' WHERE bytes > 1000"
```

The second form makes DuckDB's file readers available as a source: anything DuckDB can query, including Parquet, CSV, and JSON files, becomes a Tenzir event stream. Metadata queries such as `SELECT * FROM information_schema.tables` work the same way. Results stream, so memory use stays bounded even for large tables. In `table` mode, a downstream `select` narrows the query to the requested columns. Timestamps and dates become `time` and intervals become `duration`. Column types that TQL cannot represent, such as `DECIMAL`, `HUGEINT`, `UUID`, and `TIME`, arrive as strings with a warning. Values outside the range of `time` and `duration`, such as infinite timestamps or intervals with months, become `null` with a warning.

With `live=true`, `from_duckdb` keeps running and emits rows as other pipelines in the same node append them, using a monotonically increasing integer column to find new rows:

```tql
from_duckdb "events.duckdb", table="alerts", live=true, tracking_column="id"
```

Without `tracking_column`, the operator uses the table's single-column integer primary key. Live mode relies on new rows having larger tracking values than all rows it has already seen: a row that commits with a smaller value than one already emitted is skipped.

`to_duckdb` writes events into a table, creating it from the first event batch by default:

```tql
subscribe "alerts"
to_duckdb "events.duckdb", table="alerts", primary=id
```

`mode` selects between `create_append` (the default), `create`, and `append`, and `primary` designates a primary key column. Records become `STRUCT` columns, lists become `LIST` columns, timestamps are stored with nanosecond precision as `TIMESTAMP_NS`, durations as `INTERVAL` rounded to microseconds, and IP addresses and subnets as strings. When appending to an existing table, numbers also fit `DECIMAL` and `HUGEINT` columns and strings fit `UUID` and `ENUM` columns. Events that do not fit the table never stop the pipeline: fields without a column are dropped, and values that do not fit their column become `NULL`, such as values of another type, numbers out of range, or malformed UUIDs, each with a warning. Fields without type information, such as fields that only hold `null` or empty lists, get no column when creating the table. An event without a field for a column gets the column's `DEFAULT` value. Every batch of events is committed as soon as it is written, completely or not at all. Checkpoints additionally move committed data from DuckDB's write-ahead log into the database file; `checkpoint_interval` controls their frequency (default: 10 seconds, minimum: 1 second), and the operator checkpoints once more when the pipeline ends.

Several pipelines in the same node can write to the same database file, even to the same table, at the same time, and `from_duckdb` can read a file that `to_duckdb` is writing to. DuckDB opens a file either for reading or for writing within one process, though, so `to_duckdb` cannot open a file while a `from_duckdb` without `live=true` still reads it. Live mode opens the file for writing to avoid that, and never creates a missing file. Either way, `from_duckdb` only reads: statements in `sql` that would modify the database fail, even when it shares the database with a writer. Other processes cannot open a file while a Tenzir node writes to it. DuckDB never installs extensions on its own, and community extensions are disabled.

These operators require the Nova execution engine (`--nova`).
