# replace

Replaces the fields matching the configured extractors with fixed values.

## Synopsis

```
replace EXTRACTORS=VALUE[, …]
```

### Extractors

The extractors of fields to replace with fixed values.

### Example

Replace all values of the field named `secret` with the string value `"xxx"`,
all values of the field named `ints` with the list of integers value `[1, 2,
3]`, and all values of the field named `strs` with the list of strings value
`["a", "b", "c"]`:

```
replace secret="xxx", ints=[1, 2, 3], strs=["a", "b", "c"]
```
