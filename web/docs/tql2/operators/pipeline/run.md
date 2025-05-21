# run

Starts a pipeline in the node and waits for it to complete.

```tql
pipeline::run { … }, [id=string]
```

## Description

The `pipeline::detach` operator starts a hidden managed pipeline in the node,
and returns when the pipeline has finished.

Note that pipelines may emit diagnostics after they have finished.

:::warning Subject to Change
This operator primarily exists for testing purposes, where it is often required
to run pipelines with an explicitly specified pipeline id.
:::

### `id = string (optional)`

Sets the pipeline's ID explicitly, instead of assigning a random ID. This
corresponds to the `id` field in the output of `pipeline::list`, and the
`pipeline_id` field in the output of `metrics` and `diagnostics`.

## Examples

### Run a pipeline in the background and wait for it to complete

```tql
pipeline::run {
  every 1min {
    version
  }
  select version
  write_lines
  save_stdout
}
```

## See also

[`pipeline::detach`](detach.md)
