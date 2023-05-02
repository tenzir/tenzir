---
sidebar_position: 0
---

# Formats

A format specifies the encoding of data using two abstractions.

1. **Parser**: a component that turns raw bytes into structured event data
2. **Printer**: a component that turns structured events into raw bytes

Parsers and printers interact with their corresponding dual from a
[connector](connectors), as the diagram below shows:

![Format](format.excalidraw.svg)

The table below summarizes the parsers and printers that VAST currently
supports:

|Format|Description|Parser|Printer|
|--------|---|:----:|:--:|
|[Arrow](formats/arrow)|Apache Arrow IPC|❌|✅|
|[ASCII](formats/ascii)|Textual data representation|❌|✅|
|[CEF](formats/cef)|Common Event Format (CEF)|✅|❌|
|[CSV](formats/csv)|Comma-separated Values (CSV)|✅|✅|
|[JSON](formats/json)|Newline-delimited JSON (NDJSON)|✅|✅|
|[NetFlow](formats/netflow)|NetFlow v5, v9, and IPFIX|✅|❌|
|[PCAP](formats/pcap)|Packet handling via libpcap|✅|✅|
|[Suricata](formats/suricata)|Suricata EVE JSON|✅|❌|
|[Zeek](formats/zeek)|Zeek TSV logs|✅|✅|
|`null`|A null sink discards all data|❌|✅|
|`test`|Random event generator|✅|❌|

The list below covers all formats that VAST supports.

import DocCardList from '@theme/DocCardList';

<DocCardList />
