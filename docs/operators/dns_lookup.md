---
title: dns_lookup
category: Modify
example: "dns_lookup ip_address, result=dns_info"
---

Performs DNS lookups to resolve IP addresses to hostnames or hostnames to IP addresses.

```tql
dns_lookup field, [result=field]
```

## Description

The `dns_lookup` operator performs DNS resolution on the specified field. It automatically detects whether to perform a forward lookup (hostname to IP) or reverse lookup (IP to hostname) based on the field's content.

- **Reverse lookup**: When the field contains an IP address, the operator performs a PTR query to find the associated hostname.
- **Forward lookup**: When the field contains a string, the operator performs A and AAAA queries to find associated IP addresses.

The result is stored as a record in the specified result field.

### `field`

The field containing either an IP address or hostname to look up.

### `result = field (optional)`

The field where the DNS lookup result will be stored.

Defaults to `dns_lookup`.

The result is a record with the following structure:

For reverse lookups (IP to hostname):

```tql
{
  hostname: string
}
```

For forward lookups (hostname to IP):

```tql
{
  records: list<record>
}
```

Where each record has the structure:

```tql
{
  address: ip,
  type: string,
  ttl: duration
}
```

If the lookup fails or times out, the result field will be `null`.

## Examples

### Using default result field

When no result field is specified, the result is stored in `dns_lookup`:

```tql
from {ip: 8.8.8.8}
dns_lookup ip
```

```tql
{
  ip: 8.8.8.8,
  dns_lookup: {
    hostname: "dns.google"
  }
}
```

### Reverse DNS lookup

Resolve an IP address to its hostname:

```tql
from {src_ip: 8.8.8.8, dst_ip: 192.168.1.1}
dns_lookup src_ip, result=src_dns
```

```tql
{
  src_ip: 8.8.8.8,
  dst_ip: 192.168.1.1,
  src_dns: {
    hostname: "dns.google"
  }
}
```

### Forward DNS lookup

Resolve a hostname to its IP addresses:

```tql
from {domain: "example.com", timestamp: 2024-01-15T10:30:00}
dns_lookup domain, result=ip_info
```

```tql
{
  domain: "example.com",
  timestamp: 2024-01-15T10:30:00,
  ip_info: {
    records: [
      {address: 93.184.215.14, type: "A", ttl: 5m},
      {address: 2606:2800:21f:cb07:6820:80da:af6b:8b2c, type: "AAAA", ttl: 5m}
    ]
  }
}
```

### Handling lookup failures

When a DNS lookup fails, the result field is set to `null`:

```tql
from {ip: 192.168.1.123}
dns_lookup ip, result=hostname_info
```

```tql
{
  ip: 192.168.1.123,
  hostname_info: null
}
```

### Multiple lookups in a pipeline

```tql
from {
  source: 1.1.1.1,
  destination: "tenzir.com"
}
dns_lookup source, result=source_dns
dns_lookup destination, result=dest_ips
```

```tql
{
  source: 1.1.1.1,
  destination: "tenzir.com",
  source_dns: {
    hostname: "one.one.one.one"
  },
  dest_ips: {
    records: [
      {address: 185.199.108.153, type: "A", ttl: 1h},
      {address: 185.199.109.153, type: "A", ttl: 1h},
      {address: 185.199.110.153, type: "A", ttl: 1h},
      {address: 185.199.111.153, type: "A", ttl: 1h}
    ]
  }
}
```

## See Also

[`set`](/reference/operators/set)
