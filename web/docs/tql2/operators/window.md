# window

Cuts a pipeline into windows, running a subpipeline on each window and merging
back the results.

```tql
window [window_size=int, timeout=duration, idle_timeout=duration, parallel=int] { … }
```

## Description

The `window` operator repeats running a pipeline indefinitely at a fixed
interval. The first run starts directly when the outer pipeline itself starts.

### `window_size: int`

How many events to allow in one window at most.

Defaults to infinity.

### `timeout: duration`

The maximum duration of a single run of the pipeline.

Defaults to infinity.

:::note Difference to `every`
The `timeout` option for `window` has a subtly different behavior from `every`.
If the pipeline stops on its own, the next run will start immediately for
`window`, but for `every` will only start after the interval has elapsed.

While `every 1s { head 1 }` returns one event per second, `window timeout=1s {
head 1 }` will continuously return results.
:::

### `idle_timeout: duration`

How long the pipeline may be idle before the next run starts.

Defaults to infinity.

### `parallel: int`

Specifies how many runs of the pipeline may run in parallel if a single run
takes longer than specified through `window_size`, `timeout`, or `idle_timeout`.

Defaults to 1, i.e., no overlap.

## Examples

### Count the number of metrics per second

```tql
metrics live=true
window timeout=1s {
  summarize count=count()
}
```

```tql
{"count": 29} // after 1s
{"count": 32} // after 2s
{"count": 30} // after 3s
{"count": 30} // after 4s
// … continues like this
```

### Debounce streams with idle timeouts

The following gets the number of warnings and errors whenever there are no
diagnostics for a second, or otherwise at least once every minute.

```tql
diagnostics live=true
window timeout=1min, write_timeout=1s {
  summarize count=count(), severity
}
```

## See Also

[`every`](every.md),
[`summarize`](summarize.md)
