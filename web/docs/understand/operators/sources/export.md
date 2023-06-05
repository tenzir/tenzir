# export

Retrieves events from a VAST node. The dual to [`import`](../sinks/import.md).

## Synopsis

```
export
```

## Description

The `export` operator retrieves events from a VAST node.

:::note Flush to disk
Pipelines starting with the `export` operator do not access events that are not
written to disk.

We plan to change this behavior in the near future. Until then, we recommend
running `vast flush` before exporting events to make sure they're available for
downstream consumption.
:::

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
