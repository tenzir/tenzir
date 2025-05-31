---
title: detach
category: Pipelines
example: 'pipeline::detach { … }'
---

Starts a pipeline in the node.

```tql
pipeline::detach { … }, [id=string]
```

## Description

The `pipeline::detach` operator starts a hidden managed pipeline in the node,
and returns as soon as the pipeline has started.

:::warning[Subject to Change]
This operator primarily exists for testing purposes, where it is often required
to run pipelines in the background, but to be able to wait until the pipeline
has started. The operator may change without further notice.
:::

### `id = string (optional)`

Sets the pipeline's ID explicitly, instead of assigning a random ID. This
corresponds to the `id` field in the output of `pipeline::list`, and the
`pipeline_id` field in the output of `metrics` and `diagnostics`.

## Examples

### Run a pipeline in the background

```tql
pipeline::detach {
  every 1min {
    version
  }
  select version
  write_lines
  save_stdout
}
```

## See also

[`run`](/reference/operators/pipeline/run)
