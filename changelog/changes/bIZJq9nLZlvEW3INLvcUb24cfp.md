---
title: "Add missing `co_yield`s in `save_http`"
type: bugfix
authors: raxyte
pr: 4833
---

The TQL2 `save_http` operator had a bug causing it to fail to connect and
get stuck in an infinite loop. This is now fixed and works as expected.
