---
sidebar_custom_props:
  operator:
    transformation: true
---

# read

The `read` operator converts raw bytes into events.

## Synopsis

```
read <format>
```

## Description

The `read` operator parses events by interpreting its input bytes in a given
format.

### `<format>`

The [format](../formats.md) used to convert raw bytes into events.

Some formats have format-specific options. Please refer to the documentation of
the individual formats for more information.

## Examples

Read the input bytes as Zeek TSV logs:

```
read zeek-tsv
```

Read the input bytes as Suricata Eve JSON:

```
read suricata
```
