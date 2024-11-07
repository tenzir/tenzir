# discard

Discards all incoming events.

```tql
discard
```

## Description

The `discard` operator discards all the incoming events immediately, without
rendering them or doing any additional processing.

This operator is mainly used to test or benchmark pipelines.

## Examples

### Benchmark to see how long it takes to export everything

```tql
export 
discard
```
