# from

Produces events by combining a [connector][connectors] and a [format][formats].

## Synopsis

```
from <uri> [read <format>]
from <path> [read <format>]
from <connector> [read <format>]
```

## Description

The `from` operator produces events at the beginning of a pipeline by bringing
together a [connector][connectors] and a [format][formats].

All connectors have a default format. This enables a shorter syntax, e.g.,
`from stdin` uses the `json` format, while `from file foo.csv` uses the `csv`
format.

The `from` operator is a pipeline under the hood. For most cases, it is equal to
`load <connector> | read <format>`. However, for some combinations of
connectors and formats the underlying pipeline is a lot more complex. We
recommend always using `from ... read ...` over the [`load`](load.md) and
[`read`](../transformations/read.md) operators.

### `<connector>`

The [connector][connectors] used to load bytes.

Some connectors have connector-specific options. Please refer to the
documentation of the individual connectors for more information.

### `<format>`

The [format][formats] used to parse events from the loaded bytes.

Some formats have format-specific options. Please refer to the documentation of
the individual formats for more information.

## Examples

Read bytes from stdin and parse them as JSON.

```
from stdin read json
from file stdin read json
from file - read json
from - read json
```

Read bytes from the file `path/to/eve.json` and parse them as Suricata.
Note that the `file` connector automatically assigns the Suricata parser for
`eve.json` files when no other parser is specified.
Also, when directly passed a filesystem path, the `file` connector is automatically used.

```
from path/to/eve.json
from file path/to/eve.json
from file path/to/eve.json read suricata
```

Read bytes from the URI `https://example.com/data.json` over HTTPS and parse them as JSON.
Note that when `from` is passed a URI directly, the `https` connector is automatically used.

```
from https://example.com/data.json read json
from https example.com/data.json read json
```

[connectors]: ../../connectors.md
[formats]: ../../formats.md
