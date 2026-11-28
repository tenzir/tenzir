---
title: Filters preserve secret censoring for literal secret arguments
type: bugfix
authors:
  - mavam
created: 2026-11-28T11:49:59.494049Z
---

Filtering on a field assigned from a `secret`-typed user-defined operator parameter with a literal string argument now sees the censored `"***"` value instead of substituting the secret into the filter.

For example, in a user-defined operator taking a `secret`-typed parameter `$value`:

```tql
x = $value
where x == "***"
```

Previously, passing a literal string for `$value` caused the optimizer to rewrite `where x == "***"` against the uncensored secret, emitting a type-mismatch warning (`(secret, string)`) and dropping matching events. The filter now runs after censoring and keeps matching events with `x: "***"`, matching `secret(...)` calls.
