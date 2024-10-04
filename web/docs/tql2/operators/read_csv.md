# read_csv

Read CSV (Comma-Separated Values) from a byte stream.

```
read_csv [list_sep=str, null_value=str, comments=bool, header=str, auto_expand=bool,
          schema=str, selector=str, schema_only=bool, raw=bool, unflatten=str]
```

## Description

The `read_csv` operator transforms a byte stream into a event stream by parsing
the bytes as [CSV](https://en.wikipedia.org/wiki/Comma-separated_values).

XXX: Figure out options

### `auto_expand`

### `comments `

### `header `
The `string` to be used as a `header` for the parsed values.

### `list_sep`
The `string` to be used as a _list separator_ for the parsed values.

### `null_value`

### `raw`

### `schema`

### `selector`

### `schema_only`

### `unflatten`

## Examples

