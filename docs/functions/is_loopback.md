---
title: is_loopback
category: IP
example: 'is_loopback(127.0.0.1)'
---

Checks whether an IP address is a loopback address.

```tql
is_loopback(x:ip) -> bool
```

## Description

The `is_loopback` function checks whether a given IP address `x` is a loopback
address.

For IPv4, loopback addresses are in the range 127.0.0.0 to 127.255.255.255
(127.0.0.0/8).

For IPv6, the loopback address is ::1.

## Examples

### Check if an IP is loopback

```tql
from {
  ipv4_loopback: 127.0.0.1.is_loopback(),
  ipv6_loopback: ::1.is_loopback(),
  not_loopback: 8.8.8.8.is_loopback(),
}
```

```tql
{
  ipv4_loopback: true,
  ipv6_loopback: true,
  not_loopback: false,
}
```

## See Also

[`is_v4`](/reference/functions/is_v4),
[`is_v6`](/reference/functions/is_v6),
[`is_multicast`](/reference/functions/is_multicast),
[`is_private`](/reference/functions/is_private),
[`is_global`](/reference/functions/is_global),
[`is_link_local`](/reference/functions/is_link_local),
[`ip_type`](/reference/functions/ip_type)