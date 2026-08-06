---
title: Assign fields using dynamic keys
type: feature
authors:
  - raxyte
created: 2026-07-31T00:00:00.000000Z
---

Assignments can now use an expression to determine a field name for each
event. For example, `result[key] = value` assigns `value` to the field of
`result` named by `key`. Dynamic keys must be strings; invalid runtime keys
produce a warning and leave the affected event unchanged.
