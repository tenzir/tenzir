---
title: Correct filter optimization for mutating operators
type: bugfix
authors:
  - aljazerzen
  - codex
prs:
  - 6184
created: 2026-05-15T12:17:16.840283Z
---

Filter optimization now preserves the written semantics of pipelines that mutate events before filtering them.

For example, the following pipeline now evaluates the `where` clause after `drop`, as written:

```tql
from {foo: {bar: 1}, x: 1}
drop foo.bar
where this == {foo: {}, x: 1}
```

This also fixes optimization of pipelines that assign to `this`, and keeps filters that depend on modified fields after operators such as `drop`, `set`, and `dns_lookup`.
