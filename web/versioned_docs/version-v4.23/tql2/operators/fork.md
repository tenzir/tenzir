# fork

Executes a subpipeline with a copy of the input.

```tql
fork { … }
```

## Description

The `fork` operator execute a subpipeline with a copy its input, that is:
whenever an event arrives, it is send both to the given pipeline and forwarded
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
