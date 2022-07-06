# drop

Drops fields having the configured key suffixes from the input.

The `drop` operator is the dual to [`put`](put), which selects a given set of
fields from the output.

## Parameters

- `fields: [string]`: The key suffixes of the fields to drop.

## Example

```yaml
drop:
  fields:
    - source_ip
```
