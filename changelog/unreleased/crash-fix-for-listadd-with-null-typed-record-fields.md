---
title: Crash fix for list.add with null-typed record fields
type: bugfix
authors:
  - IyeOnline
  - claude
prs:
  - 6361
created: 2026-06-15T18:48:31.764759Z
---

`list.add` no longer crashes when the existing list contains record elements where one or more fields previously held only null values. Previously, calling `list.add` with a new element that provided a real value (for example a hostname string) for such a field would trigger an internal assertion failure. The function now correctly widens null-typed fields to accommodate the new element's type.
