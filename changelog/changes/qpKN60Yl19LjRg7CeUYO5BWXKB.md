---
title: "Improve metrics (and some other things)"
type: feature
authors: dominiklohmann
pr: 3390
---

The `sort` operator now also works for `ip` and `enum` fields.

`tenzir --dump-metrics '<pipeline>'` prints a performance overview of the
executed pipeline on stderr at the end.
