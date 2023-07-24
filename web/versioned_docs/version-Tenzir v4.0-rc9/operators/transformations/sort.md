# sort

Sorts events.

## Synopsis

```
sort <field> [<asc>|<desc>] [<nulls-first>|<nulls-last>]
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
