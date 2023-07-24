# table

Writes events in tabular form.

## Synopsis

```
table [-h|--height <int>] [-w|--width <int>] [-r|--real-time] [-T|--hide-types]
```

## Description

The `table` format renders events as table. It is the static version of the
[`explore`](../operators/sinks/explore.md) operator that displays results
in an interactive terminal user interface.

### `-h|--height <int>`

The height of the table.

Defaults to a dynamic value based on the number of rows.

### `-w|--width <int>`

The width of the table.

Defaults to a dynamic value based on the width of widest table row.

### `-r|--real-time`

Display each batch as separate table.

### `-T|--hide-types`

Do not show the type names in the header column.

## Examples

Render events as a table, one per schema:

```
write table
```

Render batches of events as they arrive, without column type annotations:

```
write table --hide-types --real-time
```
