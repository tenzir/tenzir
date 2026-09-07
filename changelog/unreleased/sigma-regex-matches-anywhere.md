---
title: Sigma regular expressions match anywhere in the value
type: bugfix
authors:
  - mavam
created: 2026-09-03T22:15:00Z
---

The `sigma` operator evaluated the `re` modifier as a full match, so a stock
rule such as `RemoteName|re: '://[0-9]{1,3}\\.[0-9]{1,3}'` never fired unless
the pattern happened to describe the entire value. Sigma defines `re` without
implicit anchors. The operator now matches regular expressions anywhere in the
value; explicit `^` and `$` anchors keep their meaning, and the patterns
derived from plain strings and wildcards are unaffected because they spell out
their anchors.
