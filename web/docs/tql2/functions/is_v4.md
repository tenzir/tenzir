# is_v4

Checks whether an IP address has version number 4.

```tql title="Synopsis"
is_v4(ip) -> str
```

## Description

The `ipv4` function checks whether the version number of an IP address is 4.

## Examples

### Check if an IP is IPv4

```tql title="Pipeline"
from {
  x: 1.2.3.4,
  y: ::1,
}
x_is_v4 = x.is_v4()
y_is_v4 = y.is_v4()
drop x, y
```

```tql title="Output"
{
  "x_is_v4": true,
  "y_is_v4": false
}
```

## See Also

[`is_v6`](is_v6.md)
