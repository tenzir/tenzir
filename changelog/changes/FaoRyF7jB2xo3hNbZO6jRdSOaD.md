---
title: "Fix `map(move foo, name, expr)`"
type: bugfix
authors: dominiklohmann
pr: 5151
---

The `move` keyword now works as expected for the first positional argument of
the `map` and `where` functions.
