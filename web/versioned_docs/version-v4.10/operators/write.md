---
sidebar_custom_props:
  operator:
    transformation: true
---

# write

The `write` operator converts events into raw bytes.

## Synopsis

```
write <format>
```

## Description

The `write` operator prints events and outputs the formatted result as raw
bytes.

### `<format>`

The [format](../formats.md) used to convert events into raw bytes.

Some formats have format-specific options. Please refer to the documentation of
the individual formats for more information.

## Examples

Convert events into JSON:

```
write json
```

Convert events into CSV:

```
write csv
```
