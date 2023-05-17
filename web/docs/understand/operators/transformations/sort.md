# sort

Sorts events.

## Synopsis

```
sort [--ascending|--descending] <field>
```

## Description

Sorts events by a provided field.

:::caution Work in Progress
The implementation of the `sort` operator currently only works with field names.
We plan to support sorting by meta data, and more generally, entire expressions.
To date, the operator also lacks support for all data types. Unsupported are
currently compound and extension types (`ip`, `subnet`, `enum`).
:::

### `<field>`

The name of the field to sort by.

### `--ascending|--descending`

Specifies the sort order.

Defaults to `--ascending` when not specified.

## Examples

Sort by the `timestamp` field in ascending order.

```
sort timestamp
```

Sort by the `timestamp` field in descending order.

```
sort --descending timestamp
```
