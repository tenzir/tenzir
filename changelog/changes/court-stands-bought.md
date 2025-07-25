---
title: "Receive UDP datagrams as events"
type: feature
authors: mavam
pr: 5375
---

The new `from_udp` operator receives UDP datagrams and outputs structured events
containing both the data and peer information.

Unlike `load_udp` which outputs raw bytes, `from_udp` produces events with
metadata about the sender, making it ideal for security monitoring and network
analysis where knowing the source of each datagram is important.

Each received datagram becomes an event with this structure:

```tql
from_udp "0.0.0.0:1234"
```

```tql
{
  data: b"Hello, UDP!\n",
  peer: {
    ip: 192.168.1.100,
    port: 54321,
    hostname: null,
  },
}
```

Enable hostname resolution for DNS lookups (disabled by default for performance):

```tql
from_udp "0.0.0.0:1234", resolve_hostnames=true
```

```tql
{
  data: b"Hello, UDP!\n",
  peer: {
    ip: 192.168.1.100,
    port: 54321,
    hostname: "client.example.com",
  },
}
```

Filter UDP data by sender and parse as Syslog afterwards:

```tql
from_udp "0.0.0.0:514"
where peer.ip in [192.168.1.0/24]
syslog = data.parse_syslog()
```
