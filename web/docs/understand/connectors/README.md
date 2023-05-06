# Connectors

A connector specifies how to exchange raw bytes with an underlying resource,
such as a file, a network socket, or a third-party library. A connector provides
a *loader* and/or *saver*:

1. **Loader**: translates raw bytes into structured event data
2. **Saver**: translates structured events into raw bytes

Loaders and savers interact with their corresponding dual from a
[format](formats):

![Connector](connector.excalidraw.svg)

Connectors appear as an argument to the [`from`](../operators/sources/from.md) and
[`to`](../operators/sinks/to.md) operators:

```
from <connector> [read <format>]
to <connector> [write <format>]
```

VAST ships with the following connectors:

import DocCardList from '@theme/DocCardList';

<DocCardList />
