---
title: "Allow missing value indices in partition flatbuffer"
type: bugfix
author: lava
created: 2022-05-20T12:40:15Z
pr: 2286
---

VAST no longer crashes when importing `map` or `pattern` data annotated with the
`#skip` attribute.
