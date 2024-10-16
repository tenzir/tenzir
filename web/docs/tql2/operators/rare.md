# rare

Shows the least common values. The dual to [`top`](top.md).

```tql
rare field [count_field=field]
```

## Description

Shows the least common values for a given field. For each unique value, a new event containing its count will be produced.

### `<field>`

The name of the field to find the least common values for.

### `count_field = field (optional)`

Field name for the counts. Defaults to `count`.

The count field and the value field must have different names.

## Examples

Find the least common values for field `id.orig_h`.

```tql
rare id.orig_h
```

Find the least common values for field `count` and present the value amount in a field `amount`.

```tql
rare count, count_field=amount
```
