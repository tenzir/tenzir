---
sidebar_custom_props:
  operator:
    source: true
---

# export

Retrieves events from a Tenzir node. The dual to [`import`](import.md).

## Synopsis

```
export [--live] [--internal]
```

## Description

The `export` operator retrieves events from a Tenzir node.

### `--live`

Work on all events that are imported with `import` operators in real-time
instead of on events persisted at a Tenzir node.

### `--internal`

Export internal events, such as metrics, instead. By default, `export` only
returns events that were previously imported with `import`. In contrast,
`export --internal` exports internal events such as pipeline metrics.
[Here](../metrics.md) is an overview of the various internal events.

:::warning Experimental
This flag and the schema of the returned events are subject to change.
:::

## Examples

Expose all persisted events as JSON data.

```
export | to stdout
```

[Apply a filter](where.md) to all persisted events, then [only expose the first
ten results](head.md).

```
export | where 1.2.3.4 | head 10 | to stdout
```
