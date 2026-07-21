---
title: Waiting for contexts
type: feature
authors:
  - IyeOnline
  - claude
prs:
  - 6461
created: 2026-07-20T22:00:13.593383Z
---

The `context::enrich` and `context::update` operators now support an
`await_context` option that waits for the target context to exist and, for
`context::enrich`, to receive its first update, instead of racing against a
context that may not exist yet or may still be empty:

```tql
from {x: 1}
context::enrich "my-context", key=x, await_context=true
```

```tql
from {x: 1, note: "populated"}
context::update "my-context", key=x, await_context=true
```

This is useful when a pipeline creates and populates a context concurrently
with (or shortly before) a `context::enrich` or `context::update` step, where
these operators previously could fail with "context not found" or, for
`context::enrich`, return misses if they ran before the context existed or
had any data.
