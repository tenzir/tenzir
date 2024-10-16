# top

Shows the most common values. The dual to [`rare`](rare.md).

```tql
top x:field
```

## Description

Shows the most common values for a given field. For each unique value, a new
event containing its count will be produced.

:::note Potentially High Memory Usage
Take care when using this operator with large inputs.
:::

### `x: field`

The field to find the most common values for.

## Examples

Find the most common values for field `id.orig_h`.

```tql
top id.orig_h
```
