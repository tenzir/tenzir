---
title: "Allow missing value indices in partition flatbuffer"
type: bugfix
authors: lava
pr: 2286
---

VAST no longer crashes when importing `map` or `pattern` data annotated with the
`#skip` attribute.
