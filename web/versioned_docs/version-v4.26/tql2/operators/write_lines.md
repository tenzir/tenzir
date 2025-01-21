# write_lines

# TODO

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
