---
sidebar_position: 0
---

# Formats

A **format** is a way data is read or written.

1. **Source**: an input format that provides a
   [reader](/docs/understand/architecture/plugins#reader)
   implementation that parses data.
2. **Sink**: an output format that provides a
   [writer](/docs/understand/architecture/plugins#writer) implementation that
   prints data.

Sources and sinks interact with a connector unless they have one built in. The
table below shows the current support of input (source) and output (sink)
formats.

|Format|Description|Input|Output|
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

Conceptually, getting data in and out of VAST involves two steps:

  1. Acquiring data from a connector by performing I/O
  2. Converting data between the connector-native format and Apache Arrow

Sometimes (1) and (2) may not be separable, e.g., when a third-party library
exposes structured data and performs the needed I/O itself. Because of this
entanglement, we treat these cases separately:

![Format](format.excalidraw.svg)

In the left case, data input and output is a blackbox and cannot be separated in
the format. In the right case, we have control about connector and format
independently, allowing us to mix and match the two.

The list below covers all formats that VAST supports.

import DocCardList from '@theme/DocCardList';

<DocCardList />
