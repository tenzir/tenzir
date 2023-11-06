# export

Retrieves events from a Tenzir node. The dual to [`import`](../sinks/import.md).

## Synopsis

```
export [--live]
```

## Description

The `export` operator retrieves events from a Tenzir node.

:::note Flush to disk
Pipelines starting with the `export` operator do not access events that are not
written to disk.

We recommend running `tenzir-ctl flush` before exporting events to make sure
they're available for downstream consumption.
:::

### `--live`

Work on all events that are imported with `import` operators in real-time
instead of on events persisted at a Tenzir node.

## Examples

Expose all persisted events as JSON data.

```
export | to stdout
```

[Apply a filter](../transformations/where.md) to all persisted events, then
[only expose the first ten results](../transformations/head.md).

```
export | where 1.2.3.4 | head 10 | to stdout
```
