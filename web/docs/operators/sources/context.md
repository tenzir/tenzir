# context create/delete

Creates or removes a context. The contexts defined via this source can be used
with the [`context` transformation](../transformations/context.md).

## Description

The `context` operator manages and applies contexts. Contexts are a flexible
mechanism for data enrichment. A context has a type and is implemented as a
`context` plugin. Each context plugin provides a specific feature set. For
example the `lookup-table` context can be used to extend events with
key-value-based contexts that consist of events passed on to the `context
update <name> key=<field>` operator call.

### `create`

Creates a new context and emits an event that represents its initial state.
The schema of that event is specific to the context type.

#### Synopsis

```
context create <name> <context-type> [--key[=]value]
```

### `delete`

Removes a context.

#### Synopsis

```
context delete <name>
```

## Examples

Create a `lookup-table` context called "mytable".

```
context create mytable lookup-table
```
