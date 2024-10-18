# batch

:::warning Expert Operator
The `batch` operator is a lower-level building block that lets users explicitly
control batching, which otherwise is controlled automatically by Tenzir's
underlying pipeline execution engine. Use with caution!
:::

The `batch` operator controls the batch size of events.

```tql
batch [limit:uint, timeout=duration]
```

## Description

The `batch` operator takes its input and rewrites it into batches of up to the
desired size.

### `limit: uint (optional)`

An unsigned integer denoting how many events to put into one batch at most.

Defaults to `65536`.

### `timeout = duration (optional)`

Specifies a maximum latency for events passing through the batch operator. When
unspecified, an infinite duration is used.
