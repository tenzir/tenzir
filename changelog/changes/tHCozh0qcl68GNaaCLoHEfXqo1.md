---
title: "Validate legacy expressions when splitting for predicate pushdown"
type: bugfix
authors: jachris
pr: 4861
---

Pipelines that begin with `export | where` followed by an expression that does
not depend on the incoming events, such as `export | where 1 == 1`, no longer
cause an internal error.
