---
sidebar_custom_props:
  operator:
    sink: true
---

# to

Consumes events by combining a [connector][connectors] and a [format][formats].

## Synopsis

```
to <uri> [write <format>]
to <path> [write <format>]
to <connector> [write <format>]
```

## Description

The `to` operator consumes events at the end of a pipeline by bringing together
a [connector][connectors] and a [format][formats].

If given something that looks like a path to a file, the connector can pick
out a format automatically based on the file extension or the file name.
This enables a shorter syntax, e.g., `to ./file.csv` uses the `csv` format.
All connectors also have a default format, which will be used
if the format can't be determined by the path. For most connectors,
this default format is `json`.
So, for example, `to stdin` uses the `json` format.

Additionally, if a file extension indicating compression can be found,
[`compress`](compress.md) is automatically used. For example, `to
myfile.json.gz` is automatically gzip-compressed and formatted as json, i.e.,
`write json | compress gzip | save myfile.json.gz`.

The `to` operator is a pipeline under the hood. For most cases, it is equal to
`write <format> | save <connector>`. However, for some combinations of
connectors and formats the underlying pipeline is a bit more complex. We
recommend always using `to ... write ...` over the [`write`](write.md) and
[`save`](save.md) operators.

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
to path/to/eve.json write json
to file path/to/eve.json write json
```

[connectors]: ../connectors.md
[formats]: ../formats.md
