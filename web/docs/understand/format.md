---
sidebar_position: 0
---

# Format

Conceptually, getting data in and out of VAST involves two steps:

  1. Acquiring data over a **carrier** medium by performing I/O
  2. Converting data between the carrier-native **format** and Apache Arrow

Sometimes (1) and (2) may not be separable, e.g., when a third-party library
exposes structured data and performs the needed I/O itself. Because of this
entanglement, we treat these cases separatetely:

![Format](/img/format.light.png#gh-light-mode-only)
![Format](/img/format.dark.png#gh-dark-mode-only)

In the left case, data input and output is a blackbox and cannot be separated in
the format. In the right case, we have control about carrier and format
independently, allowing us to mix and match the two.

A format provide can provide the following
[components](/docs/understand/architecture/components):

1. **Source**: provides a [reader](/docs/understand/architecture/plugins#reader)
   implementation that parses data.
2. **Sink**: provides a [writer](/docs/understand/architecture/plugins#writer)
   implementation prints data.

Sources and sinks compose with a carrier unless they have one built in.
The table below shows the current support of input (source) and output (sink)
formats, and whether they support choice of a carrier.

|Format|Description|Input|Output|Carrier|
|--------|---|:----:|:--:|:--:|
|`arrow`|Apache Arrow IPC|❌|✅|✅
|`ascii`|Textual data representation|❌|✅|✅
|`broker`|Zeek-native communication|✅|✅|❌
|`cef`|Common Event Format (CEF)|✅|❌|✅
|`csv`|Comma-separated Values (CSV)|✅|✅|✅
|`json`|Newline-delimited JSON (NDJSON)|✅|✅|✅
|`netflow`|NetFlow v5, v9, and IPFIX|✅|❌|✅
|`null`|A null sink discards all data|❌|✅|❌
|`pcap`|Packet handling via libpcap|✅|✅|✅
|`suricata`|Suricata EVE JSON|✅|❌|✅
|`test`|Random event data|✅|❌|❌
|`zeek`|Zeek TSV logs|✅|✅|✅
|`zeek-json`|Zeek JSON logs|✅|❌|✅
