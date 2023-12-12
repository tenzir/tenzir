---
operator:
  source: true
  transformation: true
---

# context

Manages a [context](../contexts.md).

## Synopsis

```
context create <name> <context-type>
context delete <name>
context update <name> [<options>]
```

## Description

The `context` operator manages [context](../contexts.md) instances.

- The `create` command creates a new context with a unique name. The pipeline
  returns information about the new context.

- The `delete` command destroys a given context. The pipeline returns
  information abou the deleted context.

- The `update` command adds new data to a given context. The pipeline returns
  information about what the update performed.

### `<name>`

The name of the context to create, update, or delete.

### `<context-type>`

The context type for the new context.

See the [list of available context types](../contexts.md).

### `<options>`

Context-specific options in the format `--key value` or `--flag`.

## Examples

Create a [lookup table](../contexts/lookup-table.md) context called `feodo`:

```
context create feodo lookup-table
```

Replace all previous data in the context `feodo` with data from the [Feodo
Tracker IP Block List](https://feodotracker.abuse.ch), using the `ip_address`
field as the lookup table key:

```
from https://feodotracker.abuse.ch/downloads/ipblocklist.json read json --arrays-of-objects
| context update feodo --clear --key=ip_address
```

Delete the context named `feodo`:

```
context delete feodo
```
