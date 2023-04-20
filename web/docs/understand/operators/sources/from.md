# from

Produces events by combining a [connector][connectors] and a [format][formats].

## Synopsis

```
from <connector> [read <format>]
read <format> [from <connector>]
```

## Description

The `from` operator produces events at the beginning of a pipeline by bringing
together a [connector][connectors] and a [format][formats].

Some connectors have a default format, and some formats have a default
connector. This enables a shorter syntax, e.g., `read json` uses the
`stdin` connector and `from stdin` the `json` format.

The `from` operator is a pipeline under the hood. For most cases, it is equal to
`load <connector> | parse <format>`. However, for some combinations of
connectors and formats the underlying pipeline is a lot more complex. We
recommend always using `from` or [`read`](read.md) over [`load`](load.md) and
[`parse`](../transformations/parse.md).

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

```
from file path/to/eve.json
from file path/to/eve.json read suricata
```

[connectors]: ../../connectors/README.md
[formats]: ../../formats/README.md
