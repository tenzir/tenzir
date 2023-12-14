# Connectors

A connector specifies how to exchange raw bytes with an underlying resource,
such as a file, a network socket, or a third-party library. A connector provides
a *loader* and/or *saver*:

1. **Loader**: translates raw bytes into structured event data
2. **Saver**: translates structured events into raw bytes

Loaders and savers interact with their corresponding dual from a
[format](formats):

![Connector](connectors/connector.excalidraw.svg)

Connectors appear as an argument to the [`from`](operators/from.md)
and [`to`](operators/to.md) operators:

```
from <connector> [read <format>]
to <connector> [write <format>]
```

If the format is omitted, the default depends on the connector.

Alternatively, instead of a connector, the `from` and `to` operators
can take a URL or a filesystem path directly:

```
from <url> [read <format>]
from <path> [read <format>]

to <url> [write <format>]
to <path> [write <format>]
```

When given a URL, the scheme is used to determine the connector to use.
For example, if the URL scheme is `http`, the [`http`](connectors/http.md) connector is used.
The [`gcs`](connectors/gcs.md) connector is an exception, as it will get used if the URL scheme is `gs`.

```
from https://example.com/foo.json
from https https://example.com/foo.json
from https example.com/foo.json

from gs://bucket/logs/log.json
from gcs gs://bucket/logs/log.json
```

When given a filesystem a path, the [`file`](connectors/file.md) connector is used implicitly.
To disambiguate between a relative filesystem path without any slashes or a file extension, and
a connector name, the path must contain at least one character that doesn't conform to the pattern `[A-Za-z0-9-_]`.
If the input conforms to that pattern, it's assumed to be a connector name.

```
# Parsed as a filesystem path
from /tmp/plugin.json
# `plugin` is parsed as a connector name
from plugin
# `./plugin` is parsed as a filesystem path, again (contains a `.` and a `/`)
from ./plugin
```

Tenzir ships with the following connectors:

import DocCardList from '@theme/DocCardList';

<DocCardList />
