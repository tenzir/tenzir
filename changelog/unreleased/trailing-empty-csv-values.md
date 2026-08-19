---
title: Correct parsing of trailing empty CSV values
type: bugfix
authors:
  - raxyte
created: 2026-08-19T09:31:18.053747Z
---

The CSV parser now recognizes a trailing empty value instead of mistaking it
for a missing value and emitting a warning. It parses as `null` when it matches
`null_value`, and as an empty string otherwise.
