# from

The `from` operator combines a connector with a format to create an event
source. Most pipelines start with the `from` operator.

## Synopsis

```
from <connector> [read <format>]
read <format> [from <connector>]
```

## Description

The `from` operator is the most common source of a pipeline. It is a convenience
construct that flexibly brings together a [connector][connector-docs] and a
[format][format-docs].

Some connectors have a default format, and some formats have a default
connector. This allows for a shorter syntax, e.g., `read json` will use the
`stdin` connector and `from vast` uses VAST's internal wire format.

**Trivia:** The `from` operator is a pipeline under the hood. For most cases, it
is equal to `load <connector> | parse <format>`. However, for some combinations
of connectors and formats the underlying pipeline is a lot more complex. We
recommend always using `from` or [`read`](read.md) over [`load`](load.md) and
[`parse`](../transformations/parse.md).

### `<connector>`

The [connector][connector-docs] used to load bytes or events.

Some connectors have connector-specific options. Please refer to the
documentation of the individual formats for more information.

:::tip VAST connector
The connector to read from VAST's storage engine is special in that it doesn't
load bytes that need to be parsed into VAST's wire format. Because of this, the
`vast` connector must be used without a format.
:::

### `<format>`

The [format][format-docs] used to parse events from the loaded bytes.

Some formats have format-specific options. Please refer to the documentation of
the individual formats for more information.

## Examples

Read events from VAST's storage engine.

```
from vast
```

Read bytes from stdin and parse them as JSON.

```
from stdin read json
```

Read bytes from the file `path/to/eve.json` and parse them as Suricata.

```
from file path/to/eve.json read suricata
```

[connector-docs]: ../../connectors/README.md
[format-docs]: ../../formats/README.md
