---
title: "Fix `map(move foo, name, expr)`"
type: bugfix
author: dominiklohmann
created: 2025-04-28T09:04:51Z
pr: 5151
---

The `move` keyword now works as expected for the first positional argument of
the `map` and `where` functions.
