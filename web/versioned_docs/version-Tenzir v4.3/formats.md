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

Formats appear as an argument to the [`read`](operators/sources/from.md) and
[`write`](operators/sinks/to.md) operators:

```
read <format> [from <connector>]
write <format> [to <connector>]
```

If the connector is omitted, the default is `stdin` or `stdout`.

Tenzir ships with the following formats:

import DocCardList from '@theme/DocCardList';

<DocCardList />
