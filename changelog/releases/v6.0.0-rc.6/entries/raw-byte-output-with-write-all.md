---
title: Raw byte output with write_all
type: feature
authors:
  - mavam
  - codex
created: 2026-05-15T14:48:53.630641Z
---

The new `write_all` operator concatenates one selected `string` or `blob` field
into raw bytes:

```tql
from_file "/tmp/report.pdf" {
  read_all binary=true
}
to_file "/tmp/report-copy.pdf" {
  write_all data
}
```

Use it to copy binary payloads, reconstruct byte streams after event processing,
or write string fields without separators or escaping.
