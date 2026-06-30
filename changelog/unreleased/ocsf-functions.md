---
title: "`ocsf::cast`, `ocsf::trim`, and `ocsf::derive` are now functions"
type: feature
authors:
  - jachris
created: 2026-06-30T00:00:00.000000Z
---

The OCSF operators `ocsf::cast`, `ocsf::trim`, `ocsf::derive`, and the deprecated
`ocsf::apply` are now functions that can be called in any expression, for example
`mapped = ocsf::cast(event)`. They remain usable as operators exactly as before —
`… | ocsf::cast` keeps casting, renaming, and dropping events.

When used as a function, an event that the operator form would drop (for instance
because it has no `metadata`) evaluates to `null` instead.
