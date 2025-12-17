---
title: "Expose one-level recursion for OCSF objects"
type: change
author: mavam
created: 2025-10-21T11:47:12Z
pr: 5529
---

We now support recursive OCSF objects at depth one, as opposed to dropping
recursive objects entirely. For example, pipelines can safely follow
relationships such as `process.parent_process` or `analytic.related_analytics`:

```tql
from {
  metadata: {version: "1.5.0"},
  class_uid: 1007,
  process: {
    pid: 1234,
    parent_process: {
      pid: 5678,
    },
  },
}
ocsf::apply

// New!
assert process.parent_process.pid == 5678
assert not process.parent_process.has("parent_process")
```

The first assertion now succeeds while deeper ancestry is trimmed
automatically, preserving schema compatibility for downstream consumers.
