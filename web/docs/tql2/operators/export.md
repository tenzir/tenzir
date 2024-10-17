# export

Retrieves events from a Tenzir node. The dual to [`import`](import.md).

```tql
export [live=bool, retro=bool, internal=bool, parallel=uint]
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

XXX: Is this warning still correct?
:::warning Experimental
There is a gap for live events that happen while past events are being exported.
:::

Export persistent events at a Tenzir node. Unless `live` is given, this is
implied.

Combine `retro` and `live` to export past events and live events afterwards.

### `internal = bool (optional)`

Export internal events, such as metrics or diagnostics, instead. By default,
`export` only returns events that were previously imported with `import`. In
contrast, `export internal=true` exports internal events such as operator metrics.

### `parallel = uint (optional)`

The parallel level controls how many worker threads the operator uses at most
for querying historical events.

Defaults to 3.

## Examples

Expose all persisted events as JSON data.

```tql
export
write_json
```

[Apply a filter](where.md) to all persisted events, then [only expose the first
ten results](head.md).

```tql
export 
where src_ip == 1.2.3.4 
head 10
```
