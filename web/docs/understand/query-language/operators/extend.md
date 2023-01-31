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

## Pipeline Operator String Syntax (Experimental)

```
extend FIELD=VALUE[, …]
```

### Example

Add a field named `secret` with the string value `"xxx"`, a field named `ints`
with the list of integers value `[1, 2, 3]`, and a field named `strs` with the
list of strings value `["a", "b", "c"]`:

```
extend secret="xxx", ints=[1,2,3], strs=["a","b","c"]
```
