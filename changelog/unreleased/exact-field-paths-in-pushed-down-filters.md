---
title: Exact field paths in pushed-down filters
type: bugfix
authors:
  - tobim
created: 2026-09-11T09:20:52.822385Z
---

Pushed-down filters now match complete field paths instead of matching fields by their suffix. For example, `where flow.pkts_toserver == 0` no longer matches `flow.bypassed.pkts_toserver`. This prevents unrelated nested fields from causing events to be incorrectly included or excluded.

Full schema-name prefixes remain supported for existing concept targets. After the prefix, the remaining field path must match exactly.
