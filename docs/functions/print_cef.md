---
title: print_cef
category: Printing
example: "{}.print_cef()"
---

Prints records as Common Event Format (CEF) messages

```tql
print_cef(extension:record, [cef_version=str, device_vendor=str,
          device_product=str, device_version=str, signature_id=str,
          name=str, severity=str, null_value=str]) -> str
```

## Description

Prints records as the attributes of a CEF message.

### `extension: record`

The record to print as the extension of the CEF message

### `cef_version = str (optional)`

The CEF version in the CEF header.

Defaults to the field `cef_version`, if it exists and `"0"` otherwise.

### `device_vendor = str (optional)`

The vendor in the CEF header.

Defaults to the field `device_vendor`, if it exists and `"Tenzir"` otherwise.

### `device_product = str (optional)`

The product name in the CEF header.

Defaults to the field `device_product`, if it exists and `"Tenzir Node"` otherwise.

### `device_version = str (optional)`

The product version in the CEF header.

Defaults to the field `device_version`, if it exists and the version of Tenzir
otherwise.

### `signature_id = str (optional)`

The event (class) ID in the CEF header.

Defaults to the field `signature_id`, if it exists and `"unspecified"` otherwise.

### `name = str (optional)`

The name field in the CEF header, i.e. the human readable description.

Defaults to the field `name`, if it exists and `""` otherwise.

### `severity = str (optional)`

The severity in the CEF header.

Defaults to the field `severity`, if it exists and `"Unknown"` otherwise.

### `null_value = str (optional)`

A string to use if any of the header values evaluate to null.

Defaults to an empty string.

### `flatten_separator = str (optional)`

A string used to flatten nested records in `attributes`.

Defaults to `"."`.

## Examples

### Write a CEF

```tql
from { a: 42, b: "Hello", severity_id: "critical"}
r = {a: a, b: b}.print_cef()
select r
write_lines
```

```txt
CEF:0|Tenzir|Tenzir Node|5.4.0+g5478fdbb83-dirty|unspecified|unspecified|Unknown|a=42 b=Hello
```

### Upgrade a nested CEF message in Syslog

```tql
from "my.log" {
  read_syslog // produces the expected shape for `write_syslog`
}
message = message.parse_cef() // produces the expected shape for `print_cef`
message = message.print_cef(severity="High")
write_syslog
```

## See Also

[`read_cef`](/reference/functions/read_cef),
[`parse_cef`](/reference/operators/parse_cef),
[`read_syslog`](/reference/operators/read_syslog),
[`write_syslog`](/reference/operators/write_syslog)
