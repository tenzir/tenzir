# top

Shows the most common values. The dual to [`rare`](rare.md).

```tql
top field, [count_field=field]
```

## Description

Shows the most common values for a given field. For each unique value, a new event containing its count will be produced.

### `<field>`

The field to find the most common values for.

### `count_field = field (optional)`

Field name for the counts. Defaults to `count`.

The count field and the value field must have different names.

## Examples

Find the most common values for field `id.orig_h`.

```tql
top id.orig_h
```

Find the most common values for field `count` and present the value amount in a field `amount`.

```tql
top count, count_field=amount
```
