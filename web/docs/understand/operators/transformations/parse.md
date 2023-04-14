# parse

The `parse` operator converts raw bytes into events.

:::warning Expert Operator
The `parse` operator is a lower-level building block of the
[`from`](../sources/from.md) and [`read`](../sources/read.md) operators. Only
use this if you need to operate on the raw bytes themselves.
:::

## Synopsis

```
parse <format>
```

## Description

The `parse` operator parses events by interpreting its input bytes in a given
format.

### `<format>`

The [format][formats] used to convert raw bytes into events.

Some formats have format-specific options. Please refer to the documentation of
the individual formats for more information.

## Examples

Parse the input bytes as Zeek TSV logs:

```
parse zeek-tsv
```

Parse the input bytes as Suricata Eve JSON:

```
parse suricata
```

[formats]: ../../formats/README.md
