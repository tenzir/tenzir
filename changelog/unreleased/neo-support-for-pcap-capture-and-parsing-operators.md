---
title: Neo support for PCAP capture and parsing operators
type: change
authors:
  - mavam
  - codex
pr: 6022
created: 2026-04-15T11:35:25.438939Z
---

Neo pipelines can now capture, parse, and write PCAP data with `from_nic`, `read_pcap`, and `write_pcap`.

For example, live capture now works directly in neo without a custom parser pipeline:

```tql
from_nic "en1"
```

You can also parse PCAP files or round-trip packet streams in neo:

```tql
from_file "/path/to/trace.pcap" {
  read_pcap
}
| write_pcap
```

This makes the PCAP workflow available end to end on the new executor, including live NIC capture with the default `read_pcap` parser.
