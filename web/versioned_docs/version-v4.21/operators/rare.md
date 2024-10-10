---
sidebar_custom_props:
  operator:
    transformation: true
---

# rare

Shows the least common values. The dual to [`top`](top.md).

## Synopsis

```
rare <field> [--count-field=<count-field>|-c <count-field>]
```

## Description

Shows the least common values for a given field. For each unique value, a new event containing its count will be produced.

### `<field>`

The name of the field to find the least common values for.

### `--count-field=<count-field>|-c <count-field>`

An optional argument specifying the field name of the count field. Defaults to `count`.

The count field and the value field must have different names.

## Examples

Find the least common values for field `id.orig_h`.

```
rare id.orig_h
```

Find the least common values for field `count` and present the value amount in a field `amount`.

```
rare count --count-field=amount
```
