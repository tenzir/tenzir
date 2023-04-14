# to

Consumes events by combining a [connector][connectors] and a [format][formats].

## Synopsis

```
to <connector> [write <format>]
write <format> [to <connector>]
```

## Description

The `to` operator consumes events at the end of a pipeline by bringing together
a [connector][connectors] and a [format][formats].

Some connectors have a default format, and some formats have a default
connector. This enables a shorter syntax, e.g., `write json` uses the
`stdout` connector and `to stdout` the `json` format.

The `to` operator is a pipeline under the hood. For most cases, it is equal to
`print <format> | save <connector>`. However, for some combinations of
connectors and formats the underlying pipeline is a lot more complex. We
recommend always using `to` or [`write`](write.md) over
[`print`](../transformations/print.md) and [`save`](save.md).

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
write json to file path/to/eve.json
```

[connectors]: ../../connectors/README.md
[formats]: ../../formats/README.md
