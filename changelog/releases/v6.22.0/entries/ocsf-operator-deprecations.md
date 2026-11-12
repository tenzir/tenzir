---
title: OCSF operator deprecations
type: change
authors:
  - zedoraps
created: 2026-10-27T15:05:48.592569Z
---

The `ocsf_cast`, `ocsf_trim`, and `ocsf_derive` operators are deprecated. Use their function counterparts for both top-level and nested OCSF events. Existing operator calls continue to work and emit a warning with a migration hint. To transform the whole event, assign the function result to `this`:

```tql
this = ocsf_cast(this)
this = ocsf_derive(this)
this = ocsf_trim(this)
```

Pass operator options as named function arguments, for example `this = ocsf_cast(this, null_fill=true)`. The functions return `null` for unresolved OCSF events instead of dropping them. The `ocsf_cast` function also validates required fields and constraints, and does not assign `@name`.
