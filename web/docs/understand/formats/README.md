---
sidebar_position: 0
---

# Formats

A format is the bridge between raw bytes and structured data. A format provides
a *parser* and/or *printer*:

1. **Parser**: translates raw bytes into structured event data
2. **Printer**: translates structured events into raw bytes

Parsers and printers interact with their corresponding dual from a
[connector](connectors):

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

VAST ships with the following formats:

import DocCardList from '@theme/DocCardList';

<DocCardList />
