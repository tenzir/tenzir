---
title: TQL match statements
type: feature
authors:
  - mavam
  - codex
created: 2026-05-02T15:13:13.902792Z
---

TQL now supports statement-level `match` blocks for branching on patterns:

```tql
match action {
  "accept" | "allow" => { verdict = "allowed" }
  "deny" | "drop" => { verdict = "blocked" }
  _ => { verdict = "unknown" }
}
```

Patterns can be constants, exclusive ranges, alternatives separated by `|`, or
the final wildcard `_`. Every `match` must include an unguarded final wildcard
arm, so Tenzir can prove at compile time that all possible values are covered.
This provides a concise alternative to long `else if` chains when routing events
by field value.
