# rare

Shows the least common values. The dual to [`top`](top.md).

```tql
rare x:field
```

## Description

Shows the least common values for a given field. For each unique value, a new
event containing its count will be produced.

:::note Potentially High Memory Usage
Take care when using this operator with large inputs.
:::

### `x: field`

The name of the field to find the least common values for.

## Examples

Find the least common values for field `id.orig_h`.

```tql
rare id.orig_h
```
