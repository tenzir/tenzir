---
title: Correct null handling in bloom-filter context lookups
type: bugfix
authors:
  - IyeOnline
  - claude
created: 2026-06-03T14:20:38.490452Z
---

The `bloom-filter` context no longer matches `null` values when the filter was populated with empty strings. Now null
values no longer match the context.
