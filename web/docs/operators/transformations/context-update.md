# context update

Adds new information to a context.

## Synopsis

```
context update <name> [<options>]
```

## Description

The `context update` adds new information to a context.

mechanism for data enrichment. A context has a type and is implemented as a
`context` plugin. Each context plugin provides a specific feature set. For
example the `lookup-table` context can be used to extend events with
key-value-based contexts that consist of events passed on to the `context
update <name> key=<field>` operator call.

### `<name>`

The name of the context to update.

### `<options>`

Optional, context-specific options in the format `--key value` or `--flag`.

The following options are currently supported for the `lookup-table` context:
- `--key <field>` (required): The field in the input events that is the key to
  the lookup table.
- `--clear`: Erase all entries in the lookup table before updating.

## Examples

Replace all previous data in the `lookup-table` context `feodo` with data from
the [Feodo Tracker IP Block
List](https://feodotracker.abuse.ch/downloads/ipblocklist.json), using the
`ip_address` field as the key.

```
from https://feodotracker.abuse.ch/downloads/ipblocklist.json read json --arrays-of-objects
| context update feodo --clear --key=ip_address
```
