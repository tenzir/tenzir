---
sidebar_custom_props:
  operator:
    source: true
---

# lookup

Utilize a context for filtering imported events live and exporting events
matching new additions to the context.

## Synopsis

```
lookup <context> [<options>] [--live] [--retro] [--snapshot]
lookup <field>=<context> [<options>] [--live] [--retro] [--snapshot]
```

## Description

The `lookup` operator applies a context, extending input events with a new field
defined by the context, then outputs only the events that contain this
enrichment.

For context updates, the operator searches Tenzir's storage engine for matching
events.

For imported events, the operator applies and filters using the context.

### `<context>`

The name of the context to lookup with.

### `<field>`

The name of the field in which to store the context's enrichment. Defaults to
the name of the context.

### `<options>`

Optional, context-specific options. Refer to the [`enrich` operator
documentation](enrich.md) for more details about these
options.

### `--live`

A lookup-specific flag that enables live lookup for incoming events.

By default, both retro and live lookups are enabled.
Specifying either `--retro` or `--live` explicitly disables
the other.

### `--retro`

A lookup-specific flag that enables retroactive lookups for previously imported
events. The `lookup` operator will then apply a context [after a context
update](context.md).

By default, both retro and live lookups are enabled.
Specifying either `--retro` or `--live` explicitly disables
the other.

### `--snapshot`

A lookup-specific flag that creates a snapshot of the context at the time of
execution. In combination with `--retro`, this will commence a retroactive
lookup with that current context state.

By default, snapshotting is disabled.

## Examples

Apply the `lookup-table` context `feodo` to incoming `suricata.flow` events.

```
lookup --live a --field=src_ip
| where #schema == "suricata.flow"
```

Apply the `lookup-table` context `feodo` to previous `suricata.flow` events
after an update to `feodo`.

```
lookup --retro a --field=src_ip
| where #schema == "suricata.flow"
```

Apply the `lookup-table` context `feodo` to incoming `suricata.flow` events,
then reapply the context
after an update to `feodo`.

```
lookup a --field=src_ip
| where #schema == "suricata.flow"
```
