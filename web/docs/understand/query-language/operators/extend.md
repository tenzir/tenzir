# replace

Replaces the fields matching the configured extractors with fixed values.

#### Parameters

- `fields: {extractor: value, ...}`: The fields to replace with fixed values.

#### Example

```yaml
replace:
  fields:
    secret: xxx
    ints:
      - 1
      - 2
      - 3
```
