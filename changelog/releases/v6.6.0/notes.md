This release significantly expands the `to_clickhouse` operator, which now handles LowCardinality and DateTime64 columns, tolerates columns with default values, and warns instead of silently dropping events. It also fixes a crash on empty event batches and makes pipeline shutdown more reliable.

## 🚀 Features

### Additional ClickHouse column types in `to_clickhouse`

The `to_clickhouse` operator now supports more ClickHouse column types.

When appending to an existing table, it can now write to `LowCardinality(String)` columns and to `DateTime64(N[, 'tz'])` columns of any scale and timezone. Previously both were rejected with an `unsupported ClickHouse type` error even though the data could be written without loss. A `string` is written into a `LowCardinality(String)` column (the server adds the `LowCardinality` wrapper), and `time` values are written into `DateTime64(N)` columns, truncated to the column's precision. This also covers the nullable forms `LowCardinality(Nullable(String))` and `Nullable(DateTime64(N))`.

The new `low_cardinality=` argument creates `LowCardinality(String)` columns, analogous to `json=`. Listed fields are created as `LowCardinality` instead of plain `String`:

```tql
from {id: 0, name: "alpha"}
to_clickhouse table="events", primary=id, mode="create", low_cardinality=name
// creates `name` as LowCardinality(Nullable(String))
```

Every listed field must be present in the first event, because its inner type is inferred from the data.

`LowCardinality` support is limited to string inner types: the bundled ClickHouse client library does not support `LowCardinality` over numeric or other fixed-size types.

*By @IyeOnline and @claude in #6396.*

### Reference date for partial parse_time formats

The `parse_time` function now accepts a `reference` argument for formats that don't include a year:

```tql
timestamp = timestamp.parse_time("%b %e %H:%M:%S", reference=now())
```

When the input contains month and day but no year, `parse_time` chooses the year that places the parsed timestamp closest to the reference time.

When the input contains no date fields, `parse_time` chooses the date that places the parsed time of day closest to the reference time, so times shortly before or after midnight resolve to the adjacent day. Other incomplete date formats return null and emit a warning.

Without `reference`, year-less formats keep the previous default year of 1970 for now, but emit a deprecation warning because this will become an error in a future release. If `reference` is null and a date field is missing, the result is null and `parse_time` emits a warning. Formats that include a complete date warn when `reference` is set.

*By @Zedoraps and @codex in #6380.*

### Tolerate ClickHouse columns with a default value in `to_clickhouse`

When appending to an existing ClickHouse table, `to_clickhouse` now treats any column with a default value (`DEFAULT`, `MATERIALIZED`, `ALIAS`, or `EPHEMERAL`) as optional: if the input omits it entirely, the operator leaves it out of the insert and lets ClickHouse fill in the default instead of dropping the events. Previously such a column was treated as required, so a batch that did not carry it was dropped.

This also applies to columns whose type `to_clickhouse` cannot otherwise represent, such as `IPv4` or an `Enum`. As long as they have a default, the table is no longer rejected with an `unsupported ClickHouse type` error. If an event does provide a value for such an unrepresentable column, the value cannot be converted and the event is dropped with a warning. A column with an unsupported type and no default is still rejected as before.

Generated columns (`MATERIALIZED` and `ALIAS`) are handled specially: ClickHouse computes them and rejects explicit values, so `to_clickhouse` never sends them. A value provided for such a column is ignored with a warning, and ClickHouse computes the column as defined. Only overridable `DEFAULT` (and `EPHEMERAL`) columns accept a value from the input.

*By @IyeOnline and @claude in #6396.*

## 🐞 Bug fixes

### Crash on evaluating expressions over empty event batches

Tenzir no longer crashes when a pipeline evaluates an expression over an empty batch of events. This could happen, for example, when an `accept_tcp` or `accept_http` source received a probe or partially-sent connection and passed the resulting empty batch into operators like `where`.

*By @IyeOnline and @claude in #6427.*

### Export stays fast alongside busy context enrichment

The `export` operator no longer stalls when a busy `context` enrichment pipeline (such as one using `lookup`) is reading from disk at the same time. Both used to compete for the same disk access, and a busy enrichment pipeline could make `export` dramatically slower or effectively stuck. `export` now takes priority over background enrichment reads, so it stays responsive regardless of how much enrichment work is running concurrently.

*By @IyeOnline in #6420.*

### Shutdown hangs and reports unresponsive pipelines

Some internal pipelines and pipelines that use `every` operator, might not shutdown correctly when the pipeline is stopped. This can stall the node shutdown procedure and flood the logs with "logic error" messages.

Additionally, error diagnostics from pipelines that fail while stopping are no longer lost. They now show up in the pipeline's diagnostics instead of a generic `pipeline failed` message.

*By @aljazerzen in #6428.*

### Warn instead of silently dropping events in `to_clickhouse`

`to_clickhouse` now warns instead of silently dropping events in two cases where it cannot write a row:

- An event carries a `null` for a ClickHouse column that is not nullable. The warning names the offending column.
- An event shares no column with the target table, so there is nothing to insert (an all-defaults row cannot be expressed on the native insert path).

Previously both were dropped silently while the operator reported success.

*By @IyeOnline and @claude in #6396.*
