---
title: print_leef
category: Printing
example: "{}.print_leef()"
---

[leef]: https://www.ibm.com/docs/en/dsm?topic=overview-leef-event-components

Prints records as [LEEF][leef] messages

```tql
print_leef(attributes:record, [delimiter=str, vendor=str,
           product_name=str, product_version=str, event_class_id=str,
           null_value=str, flatten_separator=str]) -> str
```

## Description

Prints records as the attributes of a [LEEF][leef] message.

### `attributes: record`

The record to print as the attributes of a LEEF message

### `delimiter = str (optional)`

This delimiter will be used to separate the key-value pairs in the attributes.
It must be a single character. If the chosen delimiter is not `"\t"`, the message
will be a LEEF:2.0 message, otherwise it will be LEEF:1.0.

Defaults to `"\t"`.

### `vendor = str (optional)`

The vendor in the LEEF header.

Defaults to the field `vendor`, if it exists and `"Tenzir"` otherwise.

### `product_name = str (optional)`

The product name in the LEEF header.

Defaults to the field `product_name`, if it exists and `"Tenzir Node"` otherwise.

### `product_version = str (optional)`

The product version in the LEEF header.

Defaults to the field `product_version`, if it exists and the version of Tenzir
otherwise.

### `event_class_id = str (optional)`

The event (class) ID in the LEEF header.

Defaults to the field `event_class_id`, if it exists and `"unspecified"` otherwise.

### `null_value = str (optional)`

A string to use if any of the header values evaluate to null.

Defaults to an empty string.

### `flatten_separator = str (optional)`

A string used to flatten nested records in `attributes`.

Defaults to `"."`.

## Examples

### Write a LEEF:1.0 message

```tql
from { a: 42, b: "Hello", event_class_id: "critical"}
r = {a: a, b: b}.print_leef()
select r
write_lines
```

```txt
LEEF:1.0|Tenzir Node|5.5.0|critical|a=42	b=Hello
```

### Reformat a nested leef message in Syslog

```tql
from "my.log" {
  read_syslog // produces the expected shape for `write_syslog`
}
message = message.parse_leef() // produces the expected shape for `print_leef`
message = message.print_leef(delimiter="^")
write_syslog
```

## See Also

[`read_leef`](/reference/functions/read_leef),
[`parse_leef`](/reference/operators/parse_leef),
[`read_syslog`](/reference/operators/read_syslog),
[`write_syslog`](/reference/operators/write_syslog)
