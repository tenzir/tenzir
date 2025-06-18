---
title: "Make `drop` not remove empty records"
type: bugfix
authors: dominiklohmann
pr: 5021
---

Dropping all fields from a record with the `drop` operator no longer removes the
record itself. For example, `from {x: {y: 0}} | drop x.y` now returns `{x: {}}`
instead of `{}`.
