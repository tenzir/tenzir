---
title: "Refactor `show` aspects and rewrite `version`"
type: change
authors: mavam
pr: 3442
---

The `version` operator no longer exists. Use `show version` to get the Tenzir
version instead. The additional information that `version` produced is now
available as `show build`, `show dependencies`, and `show plugins`.
