---
title: Fix duplicate key handling in `parse_kv` and `read_kv`
type: bugfix
authors:
  - lava
prs:
  - 6369
created: 2026-06-18T00:00:00.000000Z
---

`parse_kv` and `read_kv` now upgrade a field to a list of values when a key
occurs more than once in a single event, as documented. Previously, a repeated
key silently kept only its last value.
