---
title: "PRs 2769-2873"
type: change
author: dominiklohmann
created: 2022-12-09T20:37:19Z
pr: 2769
---

The match operator `~`, its negation `!~`, and the `pattern` type no longer
exist. Use queries of the forms `lhs == /rhs/` and `lhs != /rhs/` instead for
queries using regular expressions.
