# is_v4

Checks whether an IP address has version number 4.

```tql
is_v4(x:ip) -> bool
```

## Description

The `ipv4` function checks whether the version number of a given IP address `x`
is 4.

## Examples

### Check if an IP is IPv4

```tql
from {
  x: is_v4(1.2.3.4),
  y: is_v4(::1),
}
```

```tql
{
  x: true,
  y: false
}
```

## See Also

[`is_v6`](is_v6.md)
