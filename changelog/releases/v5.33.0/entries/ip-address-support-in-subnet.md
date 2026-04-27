---
title: IP address support in subnet
type: feature
authors:
  - mavam
  - codex
created: 2026-04-24T08:25:26.842453Z
---

The `subnet` function now accepts typed IP addresses, plain IP strings, and
existing subnet values with an optional prefix length:

```tql
from {source_ip: 10.10.1.124}
net = subnet(source_ip, 24)
```

This returns `10.10.1.0/24` without converting the IP address to a string first.
When you omit the prefix, IPv4 addresses become `/32` host subnets and IPv6
addresses become `/128` host subnets.
