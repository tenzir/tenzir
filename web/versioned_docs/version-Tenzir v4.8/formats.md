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

![Format](formats/format.excalidraw.svg)

Formats appear as an argument to the [`read`](operators/read.md)
and [`write`](operators/write.md) operators:

```
read <format>
write <format>

from <connector> [read <format>]
to <connector> [write <format>]
```

When a printer contructs raw bytes, it sets a
[MIME](https://en.wikipedia.org/wiki/Media_type) *content type* so that savers
can make assumptions about the otherwise opaque content. For example, the
[`http`](connectors/http.md) saver uses this value to populate the
`Content-Type` header when copying the raw bytes into the HTTP request body.

The builtin printers set the following MIME types:

| Format                          | MIME Type                        |
|---------------------------------|----------------------------------|
| [CSV](formats/csv.md)           | `text/csv`                       |
| [JSON](formats/json.md)         | `application/json`               |
| [NDJSON](formats/json.md)       | `application/x-ndjson`           |
| [Parquet](formats/parquet.md)   | `application/x-parquet`          |
| [PCAP](formats/pcap.md)         | `application/vnd.tcpdump.pcap`   |
| [SSV](formats/ssv.md)           | `text/plain`                     |
| [TSV](formats/tsv.md)           | `text/tab-separated-values`      |
| [YAML](formats/yaml.md)         | `application/x-yaml`             |
| [Zeek TSV](formats/zeek-tsv.md) | `application/x-zeek`             |

Tenzir ships with the following formats:

import DocCardList from '@theme/DocCardList';

<DocCardList />
