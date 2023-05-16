# Import

Ingesting data to VAST (aka *importing*) involves spinning up a VAST client
that parses and ships the data to a VAST server. In the following, we assume
that you [set up a server](../run/README.md) listening at `localhost:5158`.

## Choose an import format

Use the `import` command to ingest data from standard input or file:

```bash
vast import [options] <format> [options] [expr]
```

The [format](../../understand/formats/README.md) defines the encoding of data.
Text formats include [JSON](../../understand/formats/json.md),
[CSV](../../understand/formats/csv.md), or tool-specific data encodings like
[Zeek](../../understand/formats/zeek-tsv.md). Examples for binary formats are
[PCAP](../../understand/formats/pcap.md) and
[NetFlow](../../understand/formats/netflow.md).

For example, to import a file in JSON, use the `json` format:

```bash
vast import json < data.json
```

## Infer a schema automatically

:::caution Auto-inference coming soon
Writing a schema will be optional in the future. For example, only when you want
to tune the data semantics. Take a look at the [corresponding roadmap
item](https://github.com/tenzir/public-roadmap/issues/5) to better understand
when this capability lands in VAST.
:::

The `infer` command attempts to deduce a schema, given a sample of data. For
example, consider this JSON data:

```json
{
  "timestamp": "2011-08-14T07:38:53.914038+0200",
  "src_ip": "147.32.84.165",
  "src_port": 138,
  "dest_ip": "147.32.84.255",
  "dest_port": 138,
  "proto": "UDP"
}
```

To infer its schema, run:

```bash
jq -c < data.json | head -1 | vast infer
```

This prints:

```
type json = record {timestamp: time, src_ip: ip, src_port: int64, dest_ip: ip, dest_port: int, proto: string}
```

:::caution YAML Modules coming soon
We are currently reworking VAST's schema language. The available
[introspection capabilities](../../use/introspect/README.md) already show the
new schema style. Track the [corresponding roadmap
item](https://github.com/tenzir/public-roadmap/issues/15) to see when this
rewrite lands.
:::

The idea is that `infer` jump-starts the schema writing process by providing a
reasonable blueprint. You still need to provide the right name for the type and
perform adjustments, such as replacing some generic types with more semantic
aliases, e.g., using the `timestamp` alias instead of type `time` to designate
the event timestamp.

## Write a schema manually

If VAST does not ship with a [module][modules] for your data out of the box,
or the inference is not good enough for your use case regarding type semantics
or performance, you can easily write one yourself.

A schema is a record type with a name so that VAST can
represent it as a table internally. You would write a schema manually or extend
an existing schema if your goal is tuning type semantics and performance. For
example, if you have a field of type `string` that only holds IP addresses, you
can upgrade it to type `addr` and enjoy the benefits of richer query
expressions, e.g., top-k prefix search. Or if you onboard a new data source, you
can ship a schema along with [concept][concepts] mappings for a deeper
integration.

You write a schema (and potentially accompanying types, concepts, and models) in
a [module][modules].

Let's write one from scratch, for a tiny dummy data source called *foo* that
produces CSV events of this shape:

```csv
date,target,message
2022-05-17,10.0.0.1,foo
2022-05-18,10.0.0.2,bar
2022-05-18,10.0.0.3,bar
```

The corresponding schema type looks like this:

```yaml
message:
  record:
    - date: time
    - target: ip
    - message: msg
```

You can embed this type definition in a dedicated `foo` module:

```yaml
module: foo
types:
  message:
    record:
      - date: time
      - target: ip
      - message: msg
```

Now that you have a new module, you can choose to deploy it at the client or
the server. When a VAST server starts, it will send a copy of its local schemas
to the client. If the client has a schema for the same type, it will override
the server version. We recommend deploying the module at the server when all
clients should see the contained schemas, and at the client when the scope is
local. The diagram below illustrates the initial handshake:

![Schema Transfer](schema-transfer.excalidraw.svg)

Regardless of where you deploy the module, the procedure is the same at client
and server: place the module in an existing module directory, such as
`/etc/vast/modules`, or tell VAST in your `vast.yaml` configuration file where
to look for additional modules via the `module-dirs` key:

```yaml
vast:
  module-dirs:
    - path/to/modules
```

At the server, restart VAST and you're ready to go. Or just spin up a new client
and ingest the CSV with richer typing:

```bash
vast import csv < foo.csv
```

## Map events to schemas

For some input formats, such as JSON and CSV, VAST requires an existing schema
to find the corresponding type definition and use higher-level types.

There exist two ways to tell VAST how to map events to schemas:

1. **Field Matching**: by default, VAST checks every new record whether there
   exists a corresponding schema where the record fields match. If found, VAST
   automatically assigns the matching schema.

   The `--type=PREFIX` option makes it possible to restrict the set of candidate
   schemas to type names with a given prefix, in case there exist multiple
   schemas with identical field names. "Prefix" here means up to a dot delimiter
   or a full type name, e.g., `suricata` or `suricata.dns` are valid prefixes,
   but neither `suricat` nor `suricata.d`.

   :::info Performance Boost
   In case the prefix specified by `--type` yields *exactly one* possible
   candidate schema, VAST can operate substantially faster. The reason is that
   VAST disambiguates multiple schemas by comparing their normalized
   representation, which works by computing hash of the list of sorted field
   names and comparing it to the hash of the candidate types.
   :::

2. **Selector Specification**: some events have a dedicated field to indicate
   the type name of a particular event. For example, Suricata EVE JSON records
   have an `event_type` field that contains `flow`, `dns`, `smb`, etc., to
   signal what object structure to expect.

   To designate a selector field, use the `--selector=FIELD:PREFIX` option to
   specify a colon-separated field-name-to-schema-prefix mapping, e.g.,
   `vast import json --selector=event_type:suricata` reads the value from the
   field `event_type` and prefixes it with `suricata.` to look for a
   corresponding schema.

[types]: ../../understand/data-model/type-system.md
[concepts]: ../../understand/data-model/taxonomies.md#concepts
[modules]: ../../understand/data-model/modules.md
