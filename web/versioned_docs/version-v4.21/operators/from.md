---
sidebar_custom_props:
  operator:
    source: true
---

# from

Produces events by combining a [connector][connectors] and a [format][formats].

## Synopsis

```
from <url> [read <format>]
from <path> [read <format>]
from <connector> [read <format>]
```

## Description

The `from` operator produces events at the beginning of a pipeline by bringing
together a [connector][connectors] and a [format][formats].

If given something that looks like a path to a file, the connector can pick
out a format automatically based on the file extension or the file name.
This enables a shorter syntax, e.g., `from https://example.com/file.yml`
uses the `yaml` format. All connectors also have a default format,
which will be used if the format can't be determined by the path.
For most connectors, this default format is `json`. So, for example,
`from stdin` uses the `json` format.

Additionally, if a file extension indicating compression can be found,
[`decompress`](decompress.md) is automatically used.
For example, `from myfile.json.gz` is automatically gzip-decompressed
and parsed as json, i.e., `load myfile.json.gz | decompress gzip | read json`.

The `from` operator is a pipeline under the hood. For most cases, it is equal to
`load <connector> | read <format>`. However, for some combinations of
connectors and formats the underlying pipeline is a lot more complex. We
recommend always using `from ... read ...` over the [`load`](load.md) and
[`read`](read.md) operators.

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

Read bytes from the URL `https://example.com/data.json` over HTTPS and parse them as JSON.
Note that when `from` is passed a URL directly, the `https` connector is automatically used.

```
from https://example.com/data.json read json
from https example.com/data.json read json
```

[connectors]: ../connectors.md
[formats]: ../formats.md
