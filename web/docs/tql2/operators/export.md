# export

Retrieves events from a Tenzir node. The dual to [`import`](import.md).

```tql
export [live=bool, retro=bool, internal=bool, parallel=int]
```

## Description

The `export` operator retrieves events from a Tenzir node.

### `live = bool (optional)`

Work on all events that are imported with `import` operators in real-time
instead of on events persisted at a Tenzir node.

Note that live exports may drop events if the following pipeline fails to keep
up. To connect pipelines with back pressure, use the [`publish`](publish.md) and
[`subscribe`](subscribe.md) operators.

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

Expose all persisted events as JSON data.

```tql
export
write_json
```

Apply a filter to all persisted events, then only get the first twenty results:

```tql
export
where src_ip == 1.2.3.4
head 20
```
