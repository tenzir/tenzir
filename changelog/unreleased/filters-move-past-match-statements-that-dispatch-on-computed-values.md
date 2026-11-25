---
title: Filters move past match statements that dispatch on computed values
type: change
authors:
  - mavam
created: 2026-11-25T09:20:36.771066Z
---

A `where` after a `match` whose scrutinee is a computed expression, such as `match (name if ready else "")`, now moves above the `match` when it selects a single arm. Previously, such filters stayed behind the `match` as soon as it had more than one arm.
