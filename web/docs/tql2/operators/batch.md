# batch

:::warning Expert Operator
The `batch` operator is a lower-level building block that lets users explicitly
control batching, which otherwise is controlled automatically by Tenzir's
underlying pipeline execution engine. Use with caution!
:::

The `batch` operator controls the batch size of events.

```tql
batch [limit:int, timeout=duration]
```

## Description

The `batch` operator takes its input and rewrites it into batches of up to the
desired size.

### `limit: int (optional)`

How many events to put into one batch at most.

Defaults to `65536`.

### `timeout = duration (optional)`

Specifies a maximum latency for events passing through the batch operator. When
unspecified, an infinite duration is used.
