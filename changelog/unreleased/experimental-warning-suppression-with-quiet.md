---
title: Experimental warning suppression with quiet
type: feature
authors:
  - zedoraps
created: 2026-09-11T09:20:41.545522Z
---

The experimental `quiet` operator suppresses runtime warnings from a nested
pipeline. Use it when parsing failures are expected and you handle the resulting
null values yourself:

```tql
from {time: "2026-09-09T14:30:00Z"}, {time: "unknown"}
quiet {
  ts = time.parse_time("%Y-%m-%dT%H:%M:%SZ")
}
if ts == null {
  ts = now()
}
```

Errors and compilation diagnostics remain visible. Warnings outside the nested
pipeline are unaffected.
