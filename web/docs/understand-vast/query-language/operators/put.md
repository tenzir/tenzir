# put

Keeps the fields having the configured key suffixes and removes the rest from
the input.

## Parameters

- `fields: [string]`: The key suffixes of the fields to keep.

## Example

```yaml
- project:
    fields:
      - ip
      - timestamp
```
