---
sidebar_custom_props:
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
context reset  <name>
context save   <name>
context load   <name>
context inspect <name>
```

## Description

The `context` operator manages [context](../contexts.md) instances.

- The `create` command creates a new context with a unique name. The pipeline
  returns information about the new context.

- The `delete` command destroys a given context. The pipeline returns
  information about the deleted context.

- The `update` command adds new data to a given context. The pipeline returns
  information about what the update performed.

- The `reset` command clears the state of a given context, as if it had just
  been created. The pipeline returns information about the cleared context.

- The `save` command outputs the state of the context, serialized into bytes.
  The result can be processed further in a pipeline,
  e.g. as an input for the [`save`](./save.md) operator,
  or to initialize another context with `context load`.

- The `load` command takes in bytes, likely previously created with
  `context save`, and initializes the context with that data. The pipeline
  returns information about the populated context.

- The `inspect` command dumps a specific context's user-provided data, usually
  the context's content.

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

Inspect all data provided to `feodo`:

```
context inspect feodo
```
