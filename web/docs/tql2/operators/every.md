# every

Runs a pipeline periodically at a fixed interval.

```tql
every interval:duration, [parallel=int] { … }
```

## Description

The `every` operator repeats running a pipeline with a source indefinitely at a
fixed interval. The first run starts directly when the outer pipeline itself
starts.

If the provided pipeline does not have a source, then `every` causes the
pipeline to return intermediate results at the specified interval. This is
useful in conjunction with operators like `summarize`, which usually produces an
event when the pipeline completes, but when used with `every` returns a result
for each interval.

### `interval: duration`

Every `interval`, the executor spawns a new pipeline that runs to completion. If
the pipeline runs longer than `interval`, the next run immediately starts.

### `parallel: int`

Specifies how many runs of the pipeline may run in parallel if a single run
takes longer than the specified `interval`.

Defaults to 1, i.e., no overlap.

## Examples

### Produce one event per second and enumerate the result

```tql
every 1s {
  from {}
}
enumerate
```

```tql
{"#": 0} // immediately
{"#": 1} // after 1s
{"#": 2} // after 2s
{"#": 3} // after 3s
// … continues like this
```

### Fetch the results from an API every 10 minutes

```tql
every 10min {
  load_http "example.org/api/threats"
  read_json
}
publish "threat-feed"
```

### Get the number of metrics per second

```tql
metrics live=true
every 1s {
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

## See Also

[`cron`](cron.md),
[`window`](window.md)
