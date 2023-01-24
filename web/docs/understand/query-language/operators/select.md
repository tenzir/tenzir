# select

Keeps the fields having the configured extractors and removes the rest from the
input.

The `select` operator is the dual to [`drop`](drop), which removes a given set
of fields from the output.

## Parameters

- `fields: [string]`: The extractors of the fields to keep.

## Example

```yaml
select:
  fields:
    - ip
    - timestamp
```

## Pipeline Operator String Syntax (Experimental)

```
select FIELD[, …]
```
### Example
```
select ip, timestamp
```
