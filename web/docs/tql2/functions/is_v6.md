# is_v6

Checks whether an IP address has version number 6.

```tql title="Synopsis"
is_v6(ip) -> str
```

## Description

The `ipv6` function checks whether the version number of an IP address is 6.

## Examples

### Check if an IP is IPv6

```tql title="Pipeline"
from {
  x: 1.2.3.4,
  y: ::1,
}
x_is_v6 = x.is_v6()
y_is_v6 = y.is_v6()
drop x, y
```

```tql title="Output"
{
  "x_is_v6": false,
  "y_is_v6": true
}
```

## See Also

[`is_v4`](is_v4.md)
