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

### `<extractor>...`

The list of extractors identifying fields to keep.

## Examples

Only keep fields `foo` and `bar`:

```
select foo, bar
```

Select all fields of type `ip`:

```
select :ip
```
