---
title: Filters escaping parallel subpipelines
type: bugfix
authors:
  - aljazerzen
  - claude
prs:
  - 6270
created: 2026-06-10T07:02:07.874738Z
---

Filters inside a `parallel { … }` subpipeline no longer escape past the
`parallel` operator during optimization.

Previously, a filter such as the `where` below was lifted out of the
subpipeline and applied before `parallel`:

```tql
parallel { where x > 1 | ... }
```

The filter now stays inside the subpipeline, where it belongs, so it runs
distributed across the parallel jobs. Filters from downstream of `parallel` are
still pushed into the subpipeline as before.
