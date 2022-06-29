# put

Keeps the fields having the configured key suffixes and removes the rest from
the input.

The `put` operator is the dual to [`drop`](drop), which removes a given set of
fields from the output.

## Parameters

- `fields: [string]`: The key suffixes of the fields to keep.

## Example

```yaml
- put:
    fields:
      - ip
      - timestamp
```
