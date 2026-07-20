---
title: Await context option for enrichment
type: feature
authors:
  - IyeOnline
created: 2026-07-20T22:00:13.593383Z
---

The `context::enrich` operator now supports an `await_context` option that
waits for the target context to receive its first update before enriching,
instead of racing against a possibly still-empty context:

```tql
from {x: 1}
context::enrich "my-context", key=x, await_context=true
```

This is useful when a pipeline creates and populates a context concurrently
with (or shortly before) an `context::enrich` step, where the enrichment
previously could return misses if it ran before the context had any data.
