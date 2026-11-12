---
title: Lower CPU usage for publish metrics
type: bugfix
authors:
  - aljazerzen
created: 2026-11-12T08:37:48.307609Z
---

The `publish` operator uses less CPU when collecting per-schema metrics. Its
`schema_id` values now match those reported by `measure events` and
`type_id(this)` instead of legacy schema fingerprints.
