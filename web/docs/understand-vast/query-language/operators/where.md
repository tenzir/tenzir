# where

Keeps rows matching the configured expression and removes the rest from the
input.

## Parameters

- `expression: string`: The expression to evaluate when matching rows.

## Example

```yaml
where:
  expression: "dest_port == 53"
```
