# unroll

Returns a new event for each member of a list in an event, duplicating the
surrounding event.

```tql
unroll [field:list]
```

## Description

The `unroll` returns an event for each member of a specified list field,
leaving the surrounding event unchanged.

Drops events where the specified field is null or an empty list.

### `field: list`

Sets the name of the list field.

## Examples

### Unroll a list

```tql
from [
  {x: "a", y: [1, 2, 3]},
  {x: "b", y: []},
  {x: "c", y: [4]},
]
unroll y
```

```tql
{x: "a", y: 1}
{x: "a", y: 2}
{x: "a", y: 3}
{x: "c", y: 4}
```
