---
title: is_v6
category: IP
example: 'is_v6(::1)'
---

Checks whether an IP address has version number 6.

```tql
is_v6(x:ip) -> bool
```

## Description

The `is_v6` function checks whether the version number of a given IP address `x`
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

[`is_v4`](/reference/functions/is_v4),
[`is_multicast`](/reference/functions/is_multicast),
[`is_loopback`](/reference/functions/is_loopback),
[`is_private`](/reference/functions/is_private),
[`is_global`](/reference/functions/is_global),
[`is_link_local`](/reference/functions/is_link_local),
[`ip_category`](/reference/functions/ip_category)
