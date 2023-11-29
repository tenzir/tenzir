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

Formats appear as an argument to the [`read`](operators/transformations/read.md)
and [`write`](operators/transformations/write.md) operators:

```
read <format>
write <format>

from <connector> [read <format>]
to <connector> [write <format>]
```

Tenzir ships with the following formats:

import DocCardList from '@theme/DocCardList';

<DocCardList />
