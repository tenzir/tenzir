---
title: "Remove restriction for unflattening into empty field names"
type: bugfix
authors: dominiklohmann
pr: 3814
---

The `unflatten` operator no longer ignores fields that begin or end with the
separator.
