---
title: "Replacing values"
type: feature
author: raxyte
created: 2025-08-04T12:06:58Z
pr: 5372
---

The new `replace` operator allows you to find and replace all occurrences of a
specific value across all fields (but not in lists) in your data with another
value. This is particularly useful for data sanitization, redacting sensitive
information, or normalizing values across datasets.

The operator scans every field in each input event and replaces any value that
equals the `what` parameter with the value specified by `with`.

Replace all occurrences of the string `"-"` with null:

```tql
from {
  status: "-",
  data: {result: "-", count: 42},
  items: ["-", "valid", "-"]
}
replace what="-", with=null
```

```tql
{
  status: null,
  data: {result: null, count: 42},
  items: ["-", "valid", "-"]
}
```

Redact a specific IP address across all fields:

```tql
from {
  src_ip: 192.168.1.1,
  dst_ip: 10.0.0.1,
  metadata: {source: 192.168.1.1}
}
replace what=192.168.1.1, with="REDACTED"
```

```tql
{
  src_ip: "REDACTED",
  dst_ip: 10.0.0.1,
  metadata: {source: "REDACTED"}
}
```
