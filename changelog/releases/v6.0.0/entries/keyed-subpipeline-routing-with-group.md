---
title: Keyed subpipeline routing with `group`
type: feature
author: jachris
pr: 5980
created: 2026-04-30T12:56:38.660784Z
---

The new `group` operator routes events with the same key through a shared
subpipeline. Inside the subpipeline, `$group` refers to the key for that
subpipeline:

```tql
group tenant {
  summarize count()
}
```

The subpipeline either emits events—which are forwarded as the operator's
output—or ends with a sink, in which case `group` itself becomes a sink. Use
`group` when you need keyed routing through a stateful subpipeline, such as a
per-tenant sink or a per-session transformation. For grouped aggregations,
keep using `summarize`.
