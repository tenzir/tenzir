---
title: "Ignore events in `lookup` that did not bind"
type: bugfix
author: dominiklohmann
created: 2024-03-12T17:05:47Z
pr: 4028
---

The `lookup` operator no longer tries to match internal metrics and diagnostics
events.

The `lookup` operator no longer returns events for which none of the provided
fields exist.
