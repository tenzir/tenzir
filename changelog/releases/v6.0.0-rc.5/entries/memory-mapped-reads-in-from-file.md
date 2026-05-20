---
title: Memory-mapped reads in `from_file`
type: feature
author: raxyte
pr: 6036
created: 2026-04-30T13:00:59.26973Z
---

The `from_file` operator now accepts an `mmap=bool` option that uses
memory-mapped I/O for reading local files instead of regular reads. This can
improve performance for large files:

```tql
from_file "/var/log/large.json", mmap=true {
  read_json
}
```

Defaults to `false`.
