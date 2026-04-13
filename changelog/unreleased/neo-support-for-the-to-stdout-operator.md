---
title: Neo support for the to_stdout operator
type: feature
author: mavam
pr: 6003
created: 2026-04-13T17:19:42.452054Z
---

The neo execution engine now supports the `to_stdout` operator, so pipelines can stream byte output to standard output on the new async execution path.

For example, this pipeline now runs with neo:

```tql
from {x: 1}, {x: 2}
to_stdout {
  write_ndjson
}
```

This keeps stdout-based export and piping workflows working while moving to neo.
