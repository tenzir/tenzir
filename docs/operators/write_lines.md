---
title: write_lines
---

Writes events as key-value pairsthe *values* of an event.

```tql
write_lines
```

:::tip
Use [`write_kv`](/reference/operators/write_kv) operator if you also want to write the *key*s.
:::

## Description

Each event is printed on a new line, with fields separated by spaces,
and nulls skipped.

:::note
The lines printer does not perform any escaping. Characters like `\n` and `"`
are printed as is.
:::

## Examples

### Write the values of an event

```tql
from {x:1, y:true, z: "String"}
write_lines
```

```txt
1 true String
```

## See Also

[`read_lines`](/reference/operators/read_lines),
[`write_csv`](/reference/operators/write_csv),
[`write_kv`](/reference/operators/write_kv),
[`write_ssv`](/reference/operators/write_ssv),
[`write_tsv`](/reference/operators/write_tsv),
[`write_xsv`](/reference/operators/write_xsv)
