---
sidebar_custom_props:
  operator:
    transformation: true
---

# print

Prints the specified record field as a string.

## Synopsis

```
print <input> <printer> [<args...>]
```

## Description

The `print` operator prints a given `<input>` field of type `record` using
`<printer>` and replaces this field with the result.

### `<input>`

Specifies the field of interest. The field must be a record type.

### `<printer> [<args...>]`

Specifies the printer format and the corresponding arguments specific to each
printer.

:::info Text-based and Binary Formats
The `print` operator is currently restricted to text-based formats like JSON or
CSV. Binary formats like PCAP or Parquet are not supported.
:::

## Examples

Print [JSON](../formats/json.md) from the `flow` field in the input as
[CSV](../formats/csv.md).

```
print flow csv --no-header
```

```json {0} title="Input"
{
  "timestamp": "2021-11-17T13:32:43.237882",
  "flow_id": 852833247340038,
  "flow": {
    "pkts_toserver": 1,
    "pkts_toclient": 0,
    "bytes_toserver": 54,
    "bytes_toclient": 0,
 }
}
```

```json {0} title="Output"
{
  "timestamp": "2021-11-17T13:32:43.237882",
  "flow_id": 852833247340038,
  "flow": "1,0,54,0",
}
```
