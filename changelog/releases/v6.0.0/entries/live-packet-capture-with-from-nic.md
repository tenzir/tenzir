---
title: Live packet capture with `from_nic`
type: feature
author: mavam
pr: 6022
created: 2026-04-30T12:59:02.933262Z
---

The new `from_nic` operator captures packets from a network interface and
emits them as events directly:

```tql
from_nic "eth0"
```

Without an explicit subpipeline, `from_nic` parses the captured PCAP byte
stream with `read_pcap`. Provide a subpipeline when you want to change how
the byte stream is parsed. Use the `filter` option to apply a Berkeley Packet
Filter (BPF) expression so libpcap drops unwanted traffic before parsing:

```tql
from_nic "eth0", filter="tcp port 443"
```

The companion `read_pcap` and `write_pcap` operators have been refreshed:
`read_pcap` now also emits a `pcap.file_header` event when
`emit_file_headers=true`, which `write_pcap` consumes to preserve the
original timestamp precision and byte order. The `pcap.packet` schema's
`time.timestamp` field is now a top-level `timestamp` field, and `data` is
now a `blob`.
