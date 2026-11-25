---
title: Lower CPU usage for subscribe
type: bugfix
authors:
  - IyeOnline
created: 2026-11-18T11:24:24.396641Z
---

The `subscribe` operator uses less CPU. Its `schema_id` metrics values now
match those reported by `measure events` and `type_id(this)` instead of legacy
schema fingerprints.
