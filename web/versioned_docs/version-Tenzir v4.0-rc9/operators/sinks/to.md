# to

Consumes events by combining a [connector][connectors] and a [format][formats].

## Synopsis

```
to <connector> [write <format>]
```

## Description

The `to` operator consumes events at the end of a pipeline by bringing together
a [connector][connectors] and a [format][formats].

All connectors have a default format, which depends on the connector. This enables
a shorter syntax, e.g., `to stdout` uses the `json` format, while `to file foo.csv`
uses the `csv` format.

The `to` operator is a pipeline under the hood. For most cases, it is equal to
`write <format> | save <connector>`. However, for some combinations of
connectors and formats the underlying pipeline is a bit more complex. We
recommend always using `to ... write ...` over the
[`write`](../transformations/write.md) and [`save`](save.md) operators.

### `<connector>`

The [connector][connectors] used to save bytes.

Some connectors have connector-specific options. Please refer to the
documentation of the individual connectors for more information.

### `<format>`

The [format][formats] used to print events to bytes.

Some formats have format-specific options. Please refer to the documentation of
the individual formats for more information.

## Examples

Write events to stdout formatted as CSV.

```
to stdout write csv
```

Write events to the file `path/to/eve.json` formatted as JSON.

```
to file path/to/eve.json | write json
```

[connectors]: ../../connectors.md
[formats]: ../../formats.md
