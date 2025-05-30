---
title: "Fix TQL2 `summarize` with no groups and no input"
type: bugfix
authors: dominiklohmann
pr: 4709
---

TQL2's `summarize` now returns a single event when used with no groups and no
input events just like in TQL1, making `from [] | summarize count=count()`
return `{count: 0}` instead of nothing.
