---
title: "Remove restriction for unflattening into empty field names"
type: bugfix
author: dominiklohmann
created: 2024-01-23T14:22:18Z
pr: 3814
---

The `unflatten` operator no longer ignores fields that begin or end with the
separator.
