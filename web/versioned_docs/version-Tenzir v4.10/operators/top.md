---
sidebar_custom_props:
  operator:
    transformation: true
---

# top

Shows the most common values. The dual to [`rare`](rare.md).

## Synopsis

```
top <field> [--count-field=<count-field>|-c <count-field>]
```

## Description

Shows the most common values for a given field. For each unique value, a new event containing its count will be produced.

### `<field>`

The name of the field to find the most common values for.

### `--count-field=<count-field>|-c <count-field>`

An optional argument specifying the field name of the count field. Defaults to `count`.

The count field and the value field must have different names.

## Examples

Find the most common values for field `id.orig_h`.

```
top id.orig_h
```

Find the most common values for field `count` and present the value amount in a field `amount`.

```
top count --count-field=amount
```
