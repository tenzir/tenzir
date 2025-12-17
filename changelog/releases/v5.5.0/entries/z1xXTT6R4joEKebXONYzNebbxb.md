---
title: "Writing CEF and LEEF"
type: feature
author: IyeOnline
created: 2025-06-18T10:22:02Z
pr: 5280
---

We have added two new functions `print_leef` and `print_cef`. With
these and the already existing `write_syslog`, you are now able to write
nested CEF or LEEF in a syslog frame. In combination with the already existing
ability to read nested CEF and LEEF, this enables you to transparently forward
firewall logs.

For example, you can read in CEF messages, enrich them, and send them out
again:

```tql
// Accept syslog over TCP
load_tcp "127.0.0.1:1234" {
  read_syslog
}
// Parse the nested message as structured CEF data
message = message.parse_cef()
// Enrich the message, if its a high severity message
if message.severity in ["High", "Very High", "7", "8", "9"] {
  context::enrich "my-context",
    key=message.extension.source_ip,
    into=message.extension
}
// Re-write the message as CEF
message = message.extension.print_cef(
  cef_version=message.cef_version,
  device_vendor=message.device_vendor, device_product=message.device_product,
  device_version=message.device_version, signature_id=signature_id,
  severity=message.severity,
  name=r#"enriched via "my-context": "# + message.name
)
// Write as syslog again
write_syslog
// Send the bytestream to some destination
```
