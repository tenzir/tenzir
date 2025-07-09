---
title: is_link_local
category: IP
example: 'is_link_local(169.254.1.1)'
---

Checks whether an IP address is a link-local address.

```tql
is_link_local(x:ip) -> bool
```

## Description

The `is_link_local` function checks whether a given IP address `x` is a
link-local address.

For IPv4, link-local addresses are in the range 169.254.0.0 to 169.254.255.255
(169.254.0.0/16).

For IPv6, link-local addresses are in the range fe80::/10.

Link-local addresses are used for communication between nodes on the same
network segment and are not routable on the internet.

## Examples

### Check if an IP is link-local

```tql
from {
  ipv4_link_local: 169.254.1.1.is_link_local(),
  ipv6_link_local: fe80::1.is_link_local(),
  not_link_local: 192.168.1.1.is_link_local(),
}
```

```tql
{
  ipv4_link_local: true,
  ipv6_link_local: true,
  not_link_local: false,
}
```

## See Also

[`is_v4`](/reference/functions/is_v4),
[`is_v6`](/reference/functions/is_v6),
[`is_multicast`](/reference/functions/is_multicast),
[`is_loopback`](/reference/functions/is_loopback),
[`is_private`](/reference/functions/is_private),
[`is_global`](/reference/functions/is_global),
[`ip_category`](/reference/functions/ip_category)