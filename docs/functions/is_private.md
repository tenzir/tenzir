---
title: is_private
category: IP
example: 'is_private(192.168.1.1)'
---

Checks whether an IP address is a private address.

```tql
is_private(x:ip) -> bool
```

## Description

The `is_private` function checks whether a given IP address `x` is a private
address according to RFC 1918 (IPv4) and RFC 4193 (IPv6).

For IPv4, private addresses are:
- 10.0.0.0/8 (10.0.0.0 - 10.255.255.255)
- 172.16.0.0/12 (172.16.0.0 - 172.31.255.255)
- 192.168.0.0/16 (192.168.0.0 - 192.168.255.255)

For IPv6, private addresses are:
- fc00::/7 (Unique Local Addresses)

Note: Link-local addresses (169.254.0.0/16 for IPv4 and fe80::/10 for IPv6)
are **not** considered private addresses by this function.

## Examples

### Check if an IP is private

```tql
from {
  private_10: 10.0.0.1.is_private(),
  private_172: 172.16.0.1.is_private(),
  private_192: 192.168.1.1.is_private(),
  private_ipv6: fc00::1.is_private(),
  link_local: 169.254.1.1.is_private(),
  public: 8.8.8.8.is_private(),
}
```

```tql
{
  private_10: true,
  private_172: true,
  private_192: true,
  private_ipv6: true,
  link_local: false,
  public: false,
}
```

## See Also

[`is_v4`](/reference/functions/is_v4),
[`is_v6`](/reference/functions/is_v6),
[`is_multicast`](/reference/functions/is_multicast),
[`is_loopback`](/reference/functions/is_loopback),
[`is_global`](/reference/functions/is_global),
[`is_link_local`](/reference/functions/is_link_local),
[`ip_category`](/reference/functions/ip_category)