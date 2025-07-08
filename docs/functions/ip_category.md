---
title: ip_category
category: IP
example: 'ip_category(8.8.8.8)'
---

Returns the type classification of an IP address.

```tql
ip_category(x:ip) -> string
```

## Description

The `ip_category` function returns the category classification of a given IP address `x`
as a string. The possible return values are:

- `"unspecified"` - Unspecified address (0.0.0.0 or ::)
- `"loopback"` - Loopback address (127.0.0.0/8 or ::1)
- `"link_local"` - Link-local address (169.254.0.0/16 or fe80::/10)
- `"multicast"` - Multicast address (224.0.0.0/4 or ff00::/8)
- `"broadcast"` - Broadcast address (255.255.255.255, IPv4 only)
- `"private"` - Private address (RFC 1918 for IPv4, RFC 4193 for IPv6)
- `"global"` - Global (publicly routable) address

The function returns the most specific classification that applies to the
address. For example, 127.0.0.1 is classified as `"loopback"` rather than
just non-global.

## Examples

### Get IP address types

```tql
from {
  global_ipv4: ip_category(8.8.8.8),
  private_ipv4: ip_category(192.168.1.1),
  loopback: ip_category(127.0.0.1),
  multicast: ip_category(224.0.0.1),
  link_local: ip_category(169.254.1.1),
}
```

```tql
{
  global_ipv4: "global",
  private_ipv4: "private",
  loopback: "loopback",
  multicast: "multicast",
  link_local: "link_local",
}
```

## See Also

[`is_v4`](/reference/functions/is_v4),
[`is_v6`](/reference/functions/is_v6),
[`is_multicast`](/reference/functions/is_multicast),
[`is_loopback`](/reference/functions/is_loopback),
[`is_private`](/reference/functions/is_private),
[`is_global`](/reference/functions/is_global),
[`is_link_local`](/reference/functions/is_link_local)