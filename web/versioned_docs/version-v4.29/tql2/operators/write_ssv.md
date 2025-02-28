# write_ssv

Transforms event stream to SSV (Space-Separated Values) byte stream.

```tql
write_ssv [list_separator=str, null_value=str, no_header=bool]
```

## Description

The `write_ssv` operator transforms an event stream into a byte stream by writing
the events as SSV.

### `list_separator = str (optional)`

The string separating different elements in a list within a single field.

Defaults to `","`.

### `null_value = str (optional)`

The string denoting an absent value.

Defaults to `"-"`.

### `no_header = bool (optional)`

Whether to not print a header line containing the field names.

## Examples

Write an event as SSV.

```tql
from {x:1, y:true, z: "String"}
write_ssv
```
```
x y z
1 true String
```
