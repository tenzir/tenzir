---
title: "Improve metrics (and some other things)"
type: feature
author: dominiklohmann
created: 2023-07-24T21:33:50Z
pr: 3390
---

The `sort` operator now also works for `ip` and `enum` fields.

`tenzir --dump-metrics '<pipeline>'` prints a performance overview of the
executed pipeline on stderr at the end.
