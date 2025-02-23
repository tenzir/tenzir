# write_lines

Writes the *values* of an event.

:::tip
The [`write_kv`](write_kv.md) operator also writes the *key*s in addition
to the values.
:::

```tql
write_lines
```

## Description

Each event is printed on a new line, with fields separated by spaces,
and nulls skipped.

:::info
The lines printer does not perform any escaping. Characters like `\n` and `"`
are printed as-is.
:::

## Examples

Write the version number:

```tql
version
select version
write_lines
```

## See Also
[`read_lines`](read_lines.md), [`write_kv`](write_kv.md)
