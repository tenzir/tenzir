# lookup

Enriches a stream of events with a context.

## Synopsis

```
lookup <context> [<options>] [--live] [--retro] [--snapshot]
lookup <field=><context> [<options>] [--live] [--retro] [--snapshot]
```

## Description

The `lookup` operator applies a context, extending input events with a new field
defined by the context, then outputs only the events that contain this
enrichment.

### `<context>`

The name of the context to lookup with.

### `<field>`

The name of the field in which to store the context's enrichment. Defaults to
the name of the context.

### `<options>`

Optional, context-specific options. Refer to the [`enrich` operator
documentation](../transformations/enrich.md) for more details about these
options.

### `--live`

A lookup-specific flag that enables live lookup for incoming events.

By default, both retro and live lookups are enabled.
Specifying either `--retro` or `--live` explicitly disables
the other.

### `--retro`

A lookup-specific flag that enables retroactive lookups for previously imported
events. The `lookup` operator will then apply a context [after a context
update](../transformations/context-update.md).

By default, both retro and live lookups are enabled.
Specifying either `--retro` or `--live` explicitly disables
the other.

### `--retro`

A lookup-specific flag that creates a snapshot of the context at the time of
execution. Further live and retroactive lookups will refer to that snapshot,
disregarding any further context updates.

By default, snapshotting is disabled.

## Examples

Apply the `lookup-table` context `feodo` to incoming `suricata.flow` events.

```
lookup --live a --field=src_ip --live
| where #schema == "suricata.flow"
```

Apply the `lookup-table` context `feodo` to previous `suricata.flow` events
after an update to `feodo`.

```
lookup --live a --field=src_ip --retro
| where #schema == "suricata.flow"
```

Apply the `lookup-table` context `feodo` to incoming `suricata.flow` events,
then reapply the context
after an update to `feodo`.

```
lookup --live a --field=src_ip
| where #schema == "suricata.flow"
```
