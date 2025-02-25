# select

Selects some values and discards the rest.

```tql
select (field|assignment)...
```

## Description

This operator keeps only the provided fields and drops the rest.

### `field`

The field to keep. If it does not exist, it's given the value `null` and a
warning is emitted.

### `assignment`

An assignment of the form `<field>=<expr>`.

## Examples

### Select and create columns

Keep `a` and introduce `y` with the value of `b`:

```tql
from {a: 1, b: 2, c: 3}
select a, y=b
```

```tql
{a: 1, y: 2}
```

A more complex example with expressions and selection through records:

```tql
from {
  name: "foo",
  pos: {
    x: 1,
    y: 2,
  },
  state: "active",
}
select id=name.to_upper(), pos.x, added=true
```

```tql
{
  id: "FOO",
  pos: {
    x: 1,
  },
  added: true,
}
```
