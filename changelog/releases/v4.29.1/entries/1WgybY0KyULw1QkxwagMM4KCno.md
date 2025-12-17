---
title: "Make `drop` not remove empty records"
type: bugfix
author: dominiklohmann
created: 2025-02-27T15:55:47Z
pr: 5021
---

Dropping all fields from a record with the `drop` operator no longer removes the
record itself. For example, `from {x: {y: 0}} | drop x.y` now returns `{x: {}}`
instead of `{}`.
