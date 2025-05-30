---
title: "PRs 2769-2873"
type: change
authors: dominiklohmann
pr: 2769
---

The match operator `~`, its negation `!~`, and the `pattern` type no longer
exist. Use queries of the forms `lhs == /rhs/` and `lhs != /rhs/` instead for
queries using regular expressions.
