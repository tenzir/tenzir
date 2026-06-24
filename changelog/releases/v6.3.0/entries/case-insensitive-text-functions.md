---
title: Case-insensitive text functions
type: feature
authors:
  - aljazerzen
created: 2026-06-22T00:00:00.000000Z
---

String functions now accept an `ignore_case` argument for case-insensitive
matching, removing the need to call `to_lower()` on both sides of a
comparison. It is available on `starts_with`, `ends_with`, `contains`,
`replace`, and `split`, and defaults to `false`. Comparison uses full Unicode
case folding, so `"Aljaž".starts_with("aljaž", ignore_case=true)` is `true` and
`equals("STRASSE", "straße", ignore_case=true)` is `true`.

The new `equals(x, y, ignore_case=false)` function performs string equality
with optional case folding, covering the case-insensitive equivalent of the
`==` operator.
