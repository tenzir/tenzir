# select

Selects fields from the input. The dual to [`drop`](drop.md).

## Synopsis

```
select <extractor>...
```

## Description

The `select` operator keeps only the fields matching the provided extractors and
removes all other fields.

In relational algebra, `select` performs a *projection* of the provided
arguments.

## Options

The operator has the following options.

### extractor

An extractors identifying fields to keep.

## Examples

Keep the `timestamp` field and all fields of type `ip`:

```
select timestamp, :ip
```
