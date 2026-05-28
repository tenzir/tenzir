---
title: Arrow IPC file support in read_feather
type: bugfix
authors:
  - tobim
  - codex
pr: 6087
created: 2026-04-28T21:21:22.51495Z
---

The `read_feather` operator can now read Arrow IPC file containers in addition to Arrow IPC streams:

```tql
from_file "partition.feather" {
  read_feather
}
```

This fixes reading Tenzir archive partition files directly. If an IPC file contains a Tenzir event envelope, `read_feather` unwraps the event payload and preserves the import time; IPC files without Tenzir schema metadata use the schema name `undefined`.
