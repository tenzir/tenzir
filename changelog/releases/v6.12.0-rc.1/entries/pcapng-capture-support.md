---
title: PCAPNG capture support
type: feature
authors:
  - mavam
created: 2026-08-15T07:31:16.643981Z
---

The `read_pcap` and `write_pcap` operators now support PCAPNG captures. `read_pcap` detects PCAPNG input automatically, including captures with multiple interfaces, timestamp resolutions, byte orders, and sections. `write_pcap` preserves PCAPNG input automatically or generates PCAPNG explicitly:

```tql
write_pcap format="pcapng"
```
