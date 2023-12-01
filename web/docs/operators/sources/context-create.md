# context create

Creates a new context.

## Synopsis

```
context create <name> <context-type>
```

## Description

The `context create` creates contexts—a flexible mechanism for data enrichment.
Returns information about the created context.

### `<name>`

The name of the new context. The name must not yet be used by any other context.

### `<context-type>`

The context type for the new context. Context types are plugins.

Available options:
- `lookup-table`: an in-memory hash table with a single key column for enriching
  with arbitrary data.

## Examples

Create a `lookup-table` context called `feodo`.

```
context create feodo lookup-table
```
