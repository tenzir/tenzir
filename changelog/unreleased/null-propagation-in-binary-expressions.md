---
title: Consistent null handling in binary expressions
type: bugfix
authors:
  - mavam
  - codex
created: 2026-08-03T08:32:38.041207Z
---

TQL arithmetic operators now return `null` without a warning when either
operand is `null`. This lets expressions operate directly on optional fields:

```tql
duration = (end_time? - start_time?).count_milliseconds().round()
```

If either timestamp is missing, `duration` evaluates to `null`. Comparisons now
handle literal `null` consistently with null values in typed fields, while
unsupported operations continue to emit a warning.
