---
title: "Fix start abort error message"
type: bugfix
authors: jachris
pr: 4288
---

Errors during pipeline startup are properly propagated instead of being replaced
by `error: failed to run` in some situations.
