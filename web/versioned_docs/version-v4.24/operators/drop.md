---
sidebar_custom_props:
  operator:
    transformation: true
---

# drop

Drops fields from the input.

## Synopsis

```
drop <extractor>...
```

## Description

The `drop` operator removes all fields matching the provided extractors and
keeps all other fields. It is the dual to [`select`](select.md).

In relational algebra, `drop` performs a *projection* of the complement of the
provided arguments.

### `<extractor>...`

A comma-separated list of extractors that identify the fields to remove.

## Examples

Remove the fields `foo` and `bar`:

```
drop foo, bar
```

Remove all fields of type `ip`:

```
drop :ip
```
