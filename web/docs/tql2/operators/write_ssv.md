# write_ssv

Transforms event stream to SSV (Space-Separated Values) byte stream.

```tql
write_ssv [no_header=bool]
```

## Description

The `write_ssv` operator transforms an event stream into a byte stream by writing
the events as SSV.

### `no_header=bool (optional)`

Whether to not print a header line containing the field names.

## Examples

Write an event as SSV.

```tql
from {x:1, y:true, z: "String"}
write_ssv
```
