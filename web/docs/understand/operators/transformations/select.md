# select

Keeps the fields having the configured extractors and removes the rest from the
input.

The `select` operator is the dual to [`drop`](drop), which removes a given set
of fields from the output.

## Synopsis

```
select FIELDS[, …]
```

### Fields

The extractors of the fields to keep.

## Example

Keep the `ip` and `timestamp` fields.

```
select ip, timestamp
```
