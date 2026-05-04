---
title: TQL match statements
type: feature
authors:
  - mavam
  - codex
created: 2026-05-02T15:13:13.902792Z
---

TQL now supports statement-level `match` blocks for branching on literal values:

```tql
match action {
  "accept", "allow" => { verdict = "allowed" }
  "deny", "drop" => { verdict = "blocked" }
  _ => { verdict = "unknown" }
}
```

This provides a concise alternative to long `else if` chains when routing events by field value.
