# context

Creates or removes a context. The contexts defined via this source can be used
with the [`context` transformation](../transformations/context.md).

## Synopsis

```
context [action] <name>

actions:
  apply
  create
  remove
  update
```

## Description

The `context` operator manages and applies contexts. Contexts are a flexible
mechanism for data enrichment. A context has a type and is implemented as a
`context plugin`. Each context plugin provides a specific feature set. For
example the `constants` context can be used to extend events with fields that
are defined by the key-value-pairs passed to the `context create <name> --type
constants` operator call.

### `create`

Creates a new context and emits an event that represents it's initial state.
The schema of that event is specific to the context-type.

#### Synopsis

```
context create <name> [key=value]

options:
  --type context-type   The context type to instantiate.
```

### `remove`

Removes a context and emits a list of the remaining contexts.

#### Synopsis

```
context remove <name>
```

## Examples

Create a `constants` context called "myconstants" and that inserts the fields
"enriched" and "message".

```
context create myconstants --type constants enriched=true message="This string is constant"
```
