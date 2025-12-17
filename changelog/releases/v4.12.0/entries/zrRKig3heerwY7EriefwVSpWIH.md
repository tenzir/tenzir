---
title: "Fix `python` deadlock for empty input"
type: bugfix
author: jachris
created: 2024-04-04T07:41:36Z
pr: 4086
---

The `python` operator no longer deadlocks when given an empty program.
