---
sidebar_custom_props:
  operator:
    transformation: true
---

# enrich

Enriches events with a context.

## Synopsis

```
enrich <name>          [--field <field...>] [--replace] [--filter] [--separate]
                       [--yield <field>] [<context-options>]
enrich <output>=<name> [--field <field...>] [--filter] [--separate]
                       [--yield <field>] [<context-options>]
```

## Description

The `enrich` operator applies a context, extending input events with a new field
defined by the context.

### `<name>`

The name of the context to enrich with.

### `<output>`

The name of the field in which to store the context's enrichment. Defaults to
the name of the context.

### `--field <field...>`

A comma-separated list of fields, type extractors, or concepts to match.

### `--replace`

Replace the given fields with their respective context, omitting all
meta-information.

### `--filter`

Filter events that do not match the context.

This option is incompatible with `--replace`.

### `--separate`

When multiple fields are provided, e.g., when using `--field :ip` to enrich all
IP address fields, duplicate the event for every provided field and enrich them
individually.

When using the option, the context moves from `<output>.context.<path...>` to
`<output>` in the resulting event, with a new field `<output>.path` containing
the enriched path.

### `--yield <path>`

Provide a field into the context object to use as the context instead. If the
key does not exist within the context, a `null` value is used instead.

### `<context-options>`

Optional, context-specific options in the format `--key value` or `--flag`.
Refer to the documentation of the individual contexts for these.

## Examples

Apply the `lookup-table` context `feodo` to `suricata.flow` events, using the
`dest_ip` field as the field to compare the context key against.

```
export
| where #schema == "suricata.flow"
| enrich feodo --field dest_ip
```

To return only events that have a context, use:

```
export
| where #schema == "suricata.flow"
| enrich feodo --field dest_ip --filter
```
