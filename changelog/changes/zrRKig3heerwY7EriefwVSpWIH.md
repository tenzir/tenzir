---
title: "Fix `python` deadlock for empty input"
type: bugfix
authors: jachris
pr: 4086
---

The `python` operator no longer deadlocks when given an empty program.
