---
title: Automatic NetFlow format detection
type: change
authors:
  - mavam
  - codex
created: 2026-08-06T14:29:35.819733Z
---

`read_auto` now recognizes NetFlow v5, NetFlow v9, and IPFIX byte streams and
selects `read_netflow` automatically:

```tql
from_file "capture.nfv9" {
  read_auto
}
```
