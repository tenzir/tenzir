---
sidebar_custom_props:
  operator:
    transformation: true
---

# extend

Appends fields to events.

## Synopsis

```
extend <field=operand>...
```

## Description

The `extend` operator appends a specified list of fields to the input. All
existing fields remain intact.

The difference between `extend` and [`put`](put.md) is that `put` drops all
fields not explicitly specified, whereas `extend` only appends fields.

The difference between `extend` and [`replace`](replace.md) is that `replace`
overwrites existing fields, whereas `extend` doesn't touch the input.

The difference between `extend` and [`set`](set.md) is that `set` does not
ignore fields that do already exist in the data.

### `<field=operand>`

The assignment consists of `field` that describes the new field name and
`operand` that defines the field value.

### Examples

Add new fields with fixed values:

```
extend secret="xxx", ints=[1, 2, 3], strs=["a", "b", "c"]
```

Duplicate a column:

```
extend source=src_ip
```
