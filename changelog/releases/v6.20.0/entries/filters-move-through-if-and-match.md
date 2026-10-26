---
title: Filters move through if and match
type: change
authors:
  - mavam
created: 2026-10-01T08:14:07.004443Z
---

The optimizer now moves `where` filters in front of `if` and `match`. It rewrites the filter through each branch and routes the results by the conditions that select the branches. For example, `if x == 1 { y = a } else { y = b } | where y == 42` becomes `where a == 42 if x == 1 else b == 42` in front of the `if`. Branches that cannot keep an event drop out, so behind a `match` on the event ID whose arms assign different classes, `where class_uid == 3002` turns into a filter on the event IDs of the arms that assign `3002`.

As a result, a filter on OCSF fields now reaches the source through mappings that dispatch with `if`, `match`, or `match @name`. Filters stay inside the branches when a condition is nondeterministic, such as `random() < 0.5`.

Like a filter that moves in front of an assignment, a filter that moves in front of `if` or `match` drops events before the condition, scrutinee, or guard sees them. An error in the condition then no longer occurs for those events, but still occurs for every event that the filter keeps.
