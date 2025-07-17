---
title: print_cef
category: Printing
example: 'extension.print_cef(cef_version="0", device_vendor="Tenzir", device_product="Tenzir Node", device_version="5.5.0", signature_id=id, name="description", severity="7")'
---

Prints records as Common Event Format (CEF) messages

```tql
print_cef(extension:record, cef_version=str, device_vendor=str,
          device_product=str, device_version=str, signature_id=str,
          name=str, severity=str, [flatten_separator=str, null_value=str]) -> str
```

## Description

Prints records as the attributes of a CEF message.

### `extension: record`

The record to print as the extension of the CEF message

### `cef_version = str`

The CEF version in the CEF header.

### `device_vendor = str`

The vendor in the CEF header.

### `device_product = str`

The product name in the CEF header.

### `device_version = str`

The product version in the CEF header.

### `signature_id = str`

The event (class) ID in the CEF header.

### `name = str`

The name field in the CEF header, i.e. the human readable description.

### `severity = str`

The severity in the CEF header.

### `null_value = str (optional)`

A string to use if any of the values in `extension` are `null`.

Defaults to the empty string.

### `flatten_separator = str (optional)`

A string used to flatten nested records in `attributes`.

Defaults to `"."`.

## Examples

### Write a CEF

```tql
from {
  extension: {
    a: 42,
    b: "Hello"
  },
  signature_id: "MyCustomSignature",
  severity: "8"
}
r = extension.print_cef(
    cef_version="0",
    device_vendor="Tenzir", device_product="Tenzir Node", device_version="5.5.0",
    signature_id=signature_id, severity=severity,
    name= signature_id + " written by Tenzir"
)
select r
write_lines
```

```txt
CEF:0|Tenzir|Tenzir Node|5.5.0|MyCustomSignature|MyCustomSignature written by Tenzir|8|a=42 b=Hello
```

### Upgrade a nested CEF message in Syslog

```tql
from "my.log" {
  read_syslog // produces the expected shape for `write_syslog`
}
// read the message into a structured form
message = message.parse_cef()
// re-write the message with modifications
message = message.extension.print_cef(
  cef_version=message.cef_version,
  device_vendor=message.device_vendor, device_product=message.device_product,
  device_version=message.device_version, signature_id=signature_id, severity="9"
  name=message.name
)
write_syslog
```

## See Also

[`parse_cef`](/reference/functions/parse_cef),
[`read_cef`](/reference/operators/read_cef),
[`read_syslog`](/reference/operators/read_syslog),
[`write_syslog`](/reference/operators/write_syslog)
