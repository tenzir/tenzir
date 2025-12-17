---
title: "Fix start abort error message"
type: bugfix
author: jachris
created: 2024-06-12T12:32:11Z
pr: 4288
---

Errors during pipeline startup are properly propagated instead of being replaced
by `error: failed to run` in some situations.
