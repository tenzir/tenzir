---
title: "Perform inline DNS lookups"
type: feature
author: [mavam,IyeOnline]
created: 2025-08-11T15:27:40Z
pr: 5379
---

The new `dns_lookup` operator enables DNS resolution for both IP addresses and
domain names. It performs reverse PTR lookups for IP addresses and forward
A/AAAA lookups for hostnames, returning structured results with hostnames or IP
addresses with their types and TTLs.

Resolve a domain name to IP addresses:

```tql
from {
  host: "example.com"
}
dns_lookup host
```

```tql
{
  host: "example.com",
  dns_lookup: {
    records: [
      {
        address: 2600:1406:3a00:21::173e:2e65,
        type: "AAAA",
        ttl: 58s,
      },
      {
        address: 23.215.0.136,
        type: "A",
        ttl: 2.433333333333333min,
      },
      // ... more records
    ],
  },
}
```

Resolve an IP address to a hostname:

```tql
from {
  ip: 8.8.8.8
}
dns_lookup ip
```

```tql
{
  ip: 8.8.8.8,
  dns_lookup: {
    hostname: "dns.google",
  },
}
```
