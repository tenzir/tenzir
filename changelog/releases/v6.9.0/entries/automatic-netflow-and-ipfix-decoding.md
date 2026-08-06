---
title: Automatic NetFlow and IPFIX decoding
type: feature
authors:
  - mavam
  - codex
created: 2026-08-01T09:02:33.311724Z
---

The new `read_netflow` operator automatically decodes NetFlow v5, NetFlow v9,
and IPFIX from byte streams and binary message events, including UDP envelopes:

```tql
accept_udp "0.0.0.0:2055", binary=true
read_netflow
```

The operator learns templates independently for each exporter, emits options
records, decodes standard and enterprise information elements to native values,
and preserves unknown elements as blobs. This allows one pipeline to process
mixed NetFlow and IPFIX traffic without selecting a protocol version up front.
