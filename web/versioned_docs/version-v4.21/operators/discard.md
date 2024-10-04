---
sidebar_custom_props:
  operator:
    sink: true
---

# discard

Discards all incoming events.

## Synopsis

```
discard
```

## Description

The `discard` operator has a similar effect as `to file /dev/null write json`,
but it immediately discards all events without first rendering them with a
printer.

This operator is mainly used to test or benchmark pipelines.

## Examples

Benchmark to see how long it takes to export everything:

```
export | discard
```
