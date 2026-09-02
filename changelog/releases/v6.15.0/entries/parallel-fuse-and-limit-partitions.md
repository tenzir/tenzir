---
title: "`parallel` accepts all parallelism options"
type: feature
authors:
  - aljazerzen
created: 2026-08-26T12:00:00.000000Z
---

The `parallel` operator now accepts the same options as the `// parallelism`
directive. Use `fuse` to control how operator-to-operator channels inside the
block are wired, and `limit_partitions` to cap the degree of keyed operators
such as `summarize` and `deduplicate`:

```tql
parallel 8, fuse="none", limit_partitions=2 {
  where y > 100
  summarize g, total=sum(x)
}
```

`fuse` takes `none`, `parallel`, or `all`. Both options default to the value of
the enclosing parallelism scope, so a nested `parallel` no longer resets them.
