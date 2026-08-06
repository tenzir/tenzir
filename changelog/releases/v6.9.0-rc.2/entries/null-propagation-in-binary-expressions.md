---
title: Consistent null propagation in TQL
type: bugfix
authors:
  - mavam
  - codex
created: 2026-08-03T08:32:38.041207Z
---

TQL binary expressions now propagate `null` consistently. Arithmetic operators
return `null` without a warning when either operand is `null`. This lets
expressions operate directly on optional fields:

```tql
duration = (end_time? - start_time?).count_milliseconds().round()
```

If either timestamp is missing, `duration` evaluates to `null`. Comparisons now
handle literal `null` consistently with null values in typed fields. Other
binary operations with a `null`-typed operand also return `null` without a
warning after applying any operator-specific null semantics. Unsupported
operations on concrete non-null types still produce a warning.

Predicate positions now treat `null` as falsy without a warning. Only `true`
selects a branch, matches a guard, passes a filter, or contributes to
`count_if`. A `null` result follows the false or non-matching path but remains
`null`; it does not become the boolean value `false`. Assertions continue to
warn because they deliberately report failed invariants.
