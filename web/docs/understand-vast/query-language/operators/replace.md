# extend

Adds the configured fields with fixed values.

#### Parameters

- `fields: {field: value, ...}`: The fields to add with fixed values.

#### Example

```yaml
extend:
  fields:
    secret: xxx
    ints:
      - 1
      - 2
      - 3
```
