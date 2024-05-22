---
sidebar_custom_props:
  operator:
    transformation: true
---

# sort

Sorts events.

## Synopsis

```
sort [--stable] <field> [<asc>|<desc>] [<nulls-first>|<nulls-last>]
```

## Description

Sorts events by a provided field.

### `--stable`

Preserve the relative order of events that cannot be sorted because the provided
fields resolve to the same value.

### `<field>`

The name of the field to sort by.

### `<asc>|<desc>`

Specifies the sort order.

Defaults to `asc`.

### `<nulls-first>|<nulls-last>`

Specifies how to order null values.

Defaults to `nulls-last`.

## Examples

Sort by the `timestamp` field in ascending order.

```
sort timestamp
```

Sort by the `timestamp` field in descending order.

```
sort timestamp desc
```

Arrange by field `foo` and put null values first:

```
sort foo nulls-first
```

Arrange by field `foo` in descending order and put null values first:

```
sort foo desc nulls-first
```
