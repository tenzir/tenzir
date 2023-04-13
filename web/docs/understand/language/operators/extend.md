# extend

Adds the configured fields with fixed values.

## Synopsis

```
extend FIELD=VALUE[, …]
```

### Fields

The fields to add with fixed values.

## Example

Add a field named `secret` with the string value `"xxx"`, a field named `ints`
with the list of integers value `[1, 2, 3]`, and a field named `strs` with the
list of strings value `["a", "b", "c"]`:

```
extend secret="xxx", ints=[1, 2, 3], strs=["a", "b", "c"]
```
