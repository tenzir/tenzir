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

## Pipeline Operator String Syntax (Experimental)

```
replace FIELD=VALUE[, …]
```
### Example
```
replace secret="xxx", ints=[1,2,3], strs=["a","b","c"]
```
