# drop

Drops fields from the input. The dual to [`select`](select.md).

## Synopsis

```
drop <extractor>...
```

## Description

The `drop` operator removes all fields matching the provided extractors and
keeps all other fields.

In relational algebra, `drop` performs a *projection* of the complement of the
provided arguments.

### `<extractor>...`

A list of extractors identifying fields to remove.

## Examples

Remove the fields `foo` and `bar`:

```
drop foo, bar
```

Remove all fields of type `ip`:

```
drop :ip
```
