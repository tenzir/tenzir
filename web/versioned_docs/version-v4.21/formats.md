---
sidebar_position: 0
---

# Formats

## General

A format is the bridge between raw bytes and structured data. A format provides
a *parser* and/or *printer*:

1. **Parser**: translates raw bytes into structured event data
2. **Printer**: translates structured events into raw bytes

Parsers and printers interact with their corresponding dual from a
[connector](connectors):

![Format](formats/format.excalidraw.svg)

Formats appear as an argument to the [`read`](operators/read.md),
[`write`](operators/write.md), [`from`](operators/from.md),
[`to`](operators/to.md), [`parse`](operators/parse.md), and
[`print`](operators/print.md) operators:

```
read <format>
write <format>

from <connector> [read <format>]
to <connector> [write <format>]

parse <field> <format>
print <field> <format>
```

## Parser Schema Inference

Parsers will attempt to infer an event [schema](data-model/schemas.md) from the
input and potentially data format.
The following builtin parsers provide options for more specific control over schema inference:

- [CEF](formats/cef.md)
- [CSV](formats/csv.md)
- [GELF](formats/gelf.md)
- [JSON](formats/json.md)
- [KV](formats/kv.md)
- [LEEF](formats/leef.md)
- [Suricata](formats/suricata.md)
- [Syslog](formats/syslog.md)
- [XSV](formats/xsv.md)
- [YAML](formats/json.md)
- [Zeek JSON](formats/zeek-json.md)

The [Suricata](formats/suricata.md), [Zeek JSON](formats/zeek-json.md) and
[XSV](formats/xsv.md) parsers do not provide all of the options.

### `--merge` (Parsers)

Merges all incoming events into a single schema\* that converges over time. This
option is usually the fastest *for reading* highly heterogeneous data, but can lead
to huge schemas filled with nulls and imprecise results. Use with caution.

\*: In selector mode, only events with the same selector are merged.

This option can not be combined with `--raw --schema`.

### `--schema <schema>` (Parsers)

Explicitly set the output schema.

If a schema with a matching name is installed, the result will always have
all fields from that schema.
* Fields that are specified in the schema, but did not appear in the input will be null.
* Fields that appear in the input, but not in the schema will also be kept. `--schema-only`
can be used to reject fields that are not in the schema.

If the given schema does not exist, this option instead assigns the output schema name only.

This option can not be combined with `--selector` or `--raw --merge`.

### `--selector <field>[:<prefix>]` (Parsers)

Similar to `--schema`, but use the value of the field specified in `<field>`
as the schema name.

If the optional `<prefix>` is specified, then the schema is prepended with a
prefix. For example, the selector `event_type:suricata` with an event that has
the field `event_type` set to the value `flow` looks for a schema named
`suricata.flow`.

This option can not be combined with `--schema`.

### `--schema-only` (Parsers)

When working with an existing schema, this option will ensure that the output
schema has *only* the fields from that schema. If the schema name is obtained via a `selector`
and it does not exist, this has no effect.

This option requires either `--schema` or `--selector` to be set.

### `--unnest-separator <separator>` (Parsers)

A delimiter that, if present in keys, causes values to be treated as values of
nested records.

A popular example of this is the [Zeek JSON](formats/zeek-json.md) format. It includes
the fields `id.orig_h`, `id.orig_p`, `id.resp_h`, and `id.resp_p` at the
top-level. The data is best modeled as an `id` record with four nested fields
`orig_h`, `orig_p`, `resp_h`, and `resp_p`.

Without an unnest separator, the data looks like this:

```json
{
  "id.orig_h" : "1.1.1.1",
  "id.orig_p" : 10,
  "id.resp_h" : "1.1.1.2",
  "id.resp_p" : 5
}
```

With the unnest separator set to `.`, Tenzir reads the events like this:

```json
{
  "id" : {
    "orig_h" : "1.1.1.1",
    "orig_p" : 10,
    "resp_h" : "1.1.1.2",
    "resp_p" : 5
  }
}
```

### `--raw` (Parsers)

Use only the raw types that are native to the parsed format. Fields that have a type
specified in the chosen schema will still be parsed according to the schema.

For example, the JSON format has no notion of an IP address, so this will cause all IP addresses
to be parsed as strings, unless the field is specified to be an IP address by the schema.
JSON however has numeric types, so those would be parsed.

Use with caution.

This option can not be combined with `--merge --schema`.

## MIME Types

When a printer constructs raw bytes, it sets a
[MIME](https://en.wikipedia.org/wiki/Media_type) *content type* so that savers
can make assumptions about the otherwise opaque content. For example, the
[`http`](connectors/http.md) saver uses this value to populate the
`Content-Type` header when copying the raw bytes into the HTTP request body.

The printers set the following MIME types:

| Format                          | MIME Type                        |
|---------------------------------|----------------------------------|
| [CSV](formats/csv.md)           | `text/csv`                       |
| [JSON](formats/json.md)         | `application/json`               |
| [NDJSON](formats/json.md)       | `application/x-ndjson`           |
| [Parquet](formats/parquet.md)   | `application/x-parquet`          |
| [PCAP](formats/pcap.md)         | `application/vnd.tcpdump.pcap`   |
| [SSV](formats/ssv.md)           | `text/plain`                     |
| [TSV](formats/tsv.md)           | `text/tab-separated-values`      |
| [YAML](formats/yaml.md)         | `application/x-yaml`             |
| [Zeek TSV](formats/zeek-tsv.md) | `application/x-zeek`             |

## Available Formats

import DocCardList from '@theme/DocCardList';

<DocCardList />
