---
title: "Parse `x not in y` as `not x in y`"
type: feature
authors: raxyte
pr: 4844
---

TQL2 now allows writing `x not in y` as an equivalent to `not (x in y)` for
better readability.
