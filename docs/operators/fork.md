---
title: fork
category: Flow Control
example: 'fork { to "copy.json" }'
---

Executes a subpipeline with a copy of the input.

```tql
fork { … }
```

## Description

The `fork` operator executes a subpipeline with a copy of its input, that is:
whenever an event arrives, it is sent both to the given pipeline and forwarded
at the same time to the next operator.

### `{ … }`

The pipeline to execute. Must have a sink.

## Examples

### Publish incoming events while importing them simultaneously

```tql
fork {
  publish "imported-events"
}
import
```
