---
title: export
category: Node/Storage Engine
example: 'export'
---

Retrieves events from a Tenzir node.

```tql
export [live=bool, retro=bool, internal=bool, parallel=int]
```

## Description

The `export` operator retrieves events from a Tenzir node.

This operator is the dual to [`import`](/reference/operators/import).

### `live = bool (optional)`

Work on all events that are imported with `import` operators in real-time
instead of on events persisted at a Tenzir node.

Note that live exports may drop events if the following pipeline fails to keep
up. To connect pipelines with back pressure, use the [`publish`](/reference/operators/publish) and
[`subscribe`](/reference/operators/subscribe) operators.

### `retro = bool (optional)`

Export persistent events at a Tenzir node. Unless `live=true` is given, this is
implied.

Use `retro=true, live=true` to export past events, and live events afterwards.

### `internal = bool (optional)`

Export internal events, such as metrics or diagnostics, instead. By default,
`export` only returns events that were previously imported with `import`. In
contrast, `export internal=true` exports internal events such as operator
metrics.

### `parallel = int (optional)`

The parallel level controls how many worker threads the operator uses at most
for querying historical events.

Defaults to 3.

## Examples

### Export all stored events as JSON

```tql
export
write_json
```

### Get a subset of matching events

```tql
export
where src_ip == 1.2.3.4
head 20
```

## See Also

[`import`](/reference/operators/import),
[`subscribe`](/reference/operators/subscribe)
