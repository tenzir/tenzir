---
sidebar_custom_props:
  operator:
    transformation: true
---

# set

Upserts fields in events.

## Synopsis

```
set <field=operand>...
```

## Description

The `set` operator sets a list of fields to the given values. It overwrites old
values of fields matching the `field` expression, or creates new fields of a
given name otherwise.

### `<field=operand>`

The assignment consists of `field` that describes the new field name and
`operand` that defines the field value. If the field name already exists, the
operator replaces the value of the field.

### Examples

Upsert new fields with fixed values:

```
set secret="xxx", ints=[1, 2, 3], strs=["a", "b", "c"]
```

Move a column, replacing the old value with `null`.

```
set source=src_ip, src_ip=null
```
