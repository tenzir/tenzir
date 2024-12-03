---
sidebar_custom_props:
  operator:
    source: true
---

# lookup

Performs live filtering of the import feed using a context and translates
context updates into historical queries.

## Synopsis

```
lookup <context>          [--field <field...>] [--separate]
                          [--live] [--retro] [--yield <field>]
                          [--parallel <level>]
                          [<context-options>]
lookup <output>=<context> [--field <field...>] [--separate]
                          [--live] [--retro] [--yield <field>]
                          [--parallel <level>]
                          [<context-options>]
```

## Description

The `lookup` operator performs two actions simultaneously:

1. Translate context updates into historical queries
2. Filter all data with a context that gets ingested into a node

These two operations combined offer *unified matching*, i.e., automated retro
matching by turning context updates into historical queries, and live matching
with a context on the import feed.

The diagram below illustrates how the operator works:

![lookup](lookup.excalidraw.svg)

### `<context>`

The name of the context to lookup with.

### `<output>`

The name of the field in which to store the context's enrichment.

Defaults to the name of the context.

### `--field <field...>`

A comma-separated list of fields, type extractors, or concepts to match.

### `--separate`

When multiple fields are provided, e.g., when using `--field :ip` to enrich all
IP address fields, duplicate the event for every provided field and enrich them
individually.

When using the option, the context moves from `<output>.context.<path...>` to
`<output>` in the resulting event, with a new field `<output>.path` containing
the enriched path.

### `--live`

Enables live lookup for incoming events.

By default, both retro and live lookups are enabled. Specifying either `--retro`
or `--live` explicitly disables the other.

### `--retro`

Enables retrospective lookups for previously imported events. The `lookup`
operator will then apply a context [after a context update](context.md).

By default, both retro and live lookups are enabled.
Specifying either `--retro` or `--live` explicitly disables
the other.

### `--yield <path>`

Provide a field into the context object to use as the context instead. If the
key does not exist within the context, a `null` value is used instead.

### `--parallel <level>`

The number of partitions to open in parallel for retrospective lookups. This
number directly correlates with memory usage and performance of the `lookup`
operator.

Defaults to 3.

### `<context-options>`

Optional, context-specific options in the format `--key value` or `--flag`.
Refer to the documentation of the individual contexts for these.

## Examples

Apply the context `feodo` to incoming `suricata.flow` events.

```
lookup --live feodo --field src_ip
| where #schema == "suricata.flow"
```

Apply the context `feodo` to historical `suricata.flow` events with every update
to `feodo`.

```
lookup --retro feodo --field src_ip
| where #schema == "suricata.flow"
```

Apply the context `feodo` to incoming `suricata.flow` events, and also apply the
context after an update to `feodo`.

```
lookup feodo --field src_ip
| where #schema == "suricata.flow"
```
