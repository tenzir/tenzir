---
title: Missing events when using `in` with `export`
type: bugfix
author: raxyte
pr: 5660
created: 2026-01-14T15:06:19.05204Z
---

The `export` operator incorrectly skipped partitions when evaluating `in`
predicates with uncertain membership. This caused queries like
`export | where field in [values...]` to potentially miss matching events.
