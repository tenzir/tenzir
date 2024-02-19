---
sidebar_custom_props:
  operator:
    transformation: true
---

# enrich

Enriches events with a context.

## Synopsis

```
enrich <name> [<options>] [--filter]
enrich <field>=<name> [<options>] [--filter]
```

## Description

The `enrich` operator applies a context, extending input events with a new field
defined by the context.

### `<name>`

The name of the context to enrich with.

### `<field>`

The name of the field in which to store the context's enrichment. Defaults to
the name of the context.

### `<options>`

Optional, context-specific options in the format `--key value` or `--flag`.

The following options are currently supported for the `lookup-table` context:
- `--field <field>` (required): The field in the input events to evaluate
  against the keys of the lookup table.

### `--filter`

An optional flag that enables the operator to filter events that do not
contain a context.

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
