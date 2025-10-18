---
title: "Expose one-level recursion for OCSF objects"
type: change
authors: mavam
pr: 5529
---

We now generate `_nonrecursive` companions for recursive OCSF objects so that
pipelines can safely follow one level of relationships such as
`process.parent_process` or `analytic.related_analytics` without running into
infinitely nested records.

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
