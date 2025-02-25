# is_v6

Checks whether an IP address has version number 6.

```tql
is_v6(x:ip) -> bool
```

## Description

The `ipv6` function checks whether the version number of a given IP address `x`
is 6.

## Examples

### Check if an IP is IPv6

```tql
from {
  x: 1.2.3.4.is_v6(),
  y: ::1.is_v6(),
}
```

```tql
{
  x: false,
  y: true,
}
```

## See Also

[`is_v4`](is_v4.md)
