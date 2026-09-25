---
title: Default packet lengths in write_pcap
type: feature
authors:
  - mavam
created: 2026-09-25T06:46:25.642682Z
---

The `write_pcap` operator now fills in missing packet lengths, so you only need a link type, a timestamp, and the packet data to write a capture:

```tql
from {
  linktype: uint(1),
  timestamp: 2020-01-02T03:04:05Z,
  data: b"\x00\x01\x02",
}
@name = "pcap.packet"
write_pcap
```

A missing `captured_packet_length` defaults to the size of `data`, and a missing `original_packet_length` defaults to the captured length, which marks the packet as not truncated. Provide `original_packet_length` alone to write a truncated packet. A packet without `linktype` or `data` is an error.
