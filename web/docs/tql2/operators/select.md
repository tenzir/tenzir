# select

Selects some values and discards the rest.

```tql
select (field|assignment)...
```

## Description

The operator keeps only the provided fields from the input and drops the
rest.

### `field`

The field to keep.

### `assignment`

An `assignment` syntactically has the form `y=x`, where the result of the
expression `x` is bound to `y`.

## Examples

```tql
from { foo: "FOO", bar: 32, qux: 1ms, drops: 1000 }
select foo, bar=42, baz=qux
```
```json title="Output"
{
  "foo": "FOO",
  "bar": 42,
  "baz": "1.0ms"
}
```
