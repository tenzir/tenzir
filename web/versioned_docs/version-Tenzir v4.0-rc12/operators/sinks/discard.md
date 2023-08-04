# discard

Discards all incoming events.

:::note
This operator is mainly used to test or benchmark pipelines.
:::

## Synopsis

```
discard
```

## Description

The `discard` operator has a similar effect as `to file /dev/null write json`,
but it immediately discards all events without rendering them as JSON first.

## Examples

We can benchmark the following pipeline to see how long it takes to export
everything.

```
export | discard
```
