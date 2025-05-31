---
title: strict
category: Internals
example: 'strict { assert false }'
---
Treats all warnings as errors.

```tql
strict { … }
```

## Description

The `strict` operator takes a pipeline as an argument and treats all warnings
emitted by the execution of the pipeline as errors. This is useful when you want
to stop a pipeline on warnings or unexpected diagnostics.

## Examples

### Stop the pipeline on any warnings when sending logs

```tql
subscribe "log-feed"
strict {
  to_google_cloud_logging …
}
```

## See Also
[`assert`](/reference/operators/assert)
