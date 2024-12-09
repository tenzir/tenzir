---
sidebar_custom_props:
  operator:
    transformation: true
---

# select

Selects fields from the input.

## Synopsis

```
select <extractor>...
```

## Description

The `select` operator keeps only the fields matching the provided extractors and
removes all other fields. It is the dual to [`drop`](drop.md).

In relational algebra, `select` performs a *projection* of the provided
arguments.

### `<extractor>...`

A comma-separated list of extractors that identify the fields to keep.

## Examples

Only keep fields `foo` and `bar`:

```
select foo, bar
```

Select all fields of type `ip`:

```
select :ip
```
