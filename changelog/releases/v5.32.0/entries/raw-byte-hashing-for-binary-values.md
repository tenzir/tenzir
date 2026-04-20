---
title: Raw-byte hashing for binary values
type: bugfix
authors:
  - mavam
  - codex
pr: 6022
created: 2026-04-16T07:29:26.558216Z
---

The `hash_*` functions now hash `blob` values by their raw bytes. This makes checksums computed from binary data match external tools such as `md5sum` and `sha256sum`.

For example:

```tql
from_file "trace.pcap" {
  read_all binary=true
}
md5 = data.hash_md5()
```

This is useful for verifying file contents and round-tripping binary formats without leaving TQL.
