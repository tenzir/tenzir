---
title: "IP address categorization functions"
type: feature
author: mavam
created: 2025-07-08T17:50:05Z
pr: 5336
---

Ever wondered if that suspicious traffic is coming from inside the corporate
network? 🏢 We've got you covered with a new suite of IP address classification
functions that make network analysis a breeze.

**`is_private()`** - Quickly spot internal RFC 1918 addresses in your logs.
Perfect for identifying lateral movement or distinguishing between internal and
external threats:

```tql
where src_ip.is_private() and dst_ip.is_global()
// Catch data exfiltration attempts from your internal network
```

**`is_global()`** - Find publicly routable addresses. Essential for tracking
external attackers or monitoring outbound connections:

```tql
where src_ip.is_global() and failed_login_count > 5
// Detect brute force attempts from the internet
```

**`is_multicast()`** - Identify multicast traffic (224.0.0.0/4, ff00::/8).
Great for spotting mDNS, SSDP, and other broadcast protocols that shouldn't
cross network boundaries:

```tql
where dst_ip.is_multicast() and src_ip.is_global()
// Flag suspicious multicast from external sources
```

**`is_link_local()`** - Detect link-local addresses (169.254.0.0/16,
fe80::/10). Useful for identifying misconfigurations or APIPA fallback:

```tql
where server_ip.is_link_local()
// Find services accidentally binding to link-local addresses
```

**`is_loopback()`** - Spot loopback addresses (127.0.0.0/8, ::1). Hunt for
suspicious local connections or tunneled traffic:

```tql
where src_ip != dst_ip and dst_ip.is_loopback()
// Unusual loopback connections might indicate malware
```

**`ip_category()`** - Get the complete classification in one shot. Returns:
"global", "private", "multicast", "link_local", "loopback", "broadcast", or
"unspecified":

```tql
where src_ip.ip_category() == "private" and dst_ip.ip_category() == "multicast"
// Analyze traffic patterns by IP category
```

These functions work seamlessly with both IPv4 and IPv6 addresses, making them
future-proof for your dual-stack environments. Happy hunting! 🔍
