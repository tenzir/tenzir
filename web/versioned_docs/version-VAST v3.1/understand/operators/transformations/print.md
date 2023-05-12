# print

The `print` operator converts raw bytes into events.

:::warning Expert Operator
The `print` operator is a lower-level building block of the
[`to`](../sinks/to.md) and [`write`](../sinks/write.md) operators. Only use this
if you need to operate on the raw bytes themselves.
:::

## Synopsis

```
print <format>
```

## Description

The `print` operator prints events and outputs the formatted result as raw
bytes.

### `<format>`

The [format][formats] used to convert events into raw bytes.

Some formats have format-specific options. Please refer to the documentation of
the individual formats for more information.

## Examples

Convert events into JSON:

```
print json
```

Convert events into CSV:

```
print csv
```

[formats]: ../../formats/README.md
