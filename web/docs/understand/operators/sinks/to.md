# to

The `to` operator combines a connector with a format to create an event
sink. Most pipelines end with the `to` operator.

## Synopsis

```
to <connector> [write <format>]
write <format> [to <connector>]
```

## Description

The `to` operator is the most common source of a pipeline. It is a convenience
construct that flexibly brings together a [connector][connector-docs] and a
[format][format-docs].

Some connectors have a default format, and some formats have a default
connector. This allows for a shorter syntax, e.g., `write json` will use the
`stdout` connector and `to vast` uses VAST's internal wire format.

**Trivia:** The `to` operator is a pipeline under the hood. For most cases, it
is equal to `print <format> | dump <connector>`. However, for some combinations
of connectors and formats the underlying pipeline is a lot more complex. We
recommend always using `to` or [`write`](write.md) over
[`print`](../transformations/print.md) and [`dump`](dump.md).

### `<connector>`

The [connector][connector-docs] used to dump bytes or events.

Some connectors have connector-specific options. Please refer to the
documentation of the individual formats for more information.

:::tip VAST connector
The connector to write to VAST's storage engine is special in that it doesn't
dmp bytes needed to be parsed into VAST's wire format. Because of this, the
`vast` connector must be used without a format.
:::

### `<format>`

The [format][format-docs] used to print events.

Some formats have format-specific options. Please refer to the documentation of
the individual formats for more information.

## Examples

Write events to VAST's storage engine.

```
to vast
```

Write bytes to stdout formatted as JSON.

```
to stdout write json
```

Write bytes to the file `path/to/eve.json` formatted as JSON.

```
to file path/to/eve.json write json
```

[connector-docs]: ../../connectors/README.md
[format-docs]: ../../formats/README.md
