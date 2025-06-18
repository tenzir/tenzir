---
title: "Ignore events in `lookup` that did not bind"
type: bugfix
authors: dominiklohmann
pr: 4028
---

The `lookup` operator no longer tries to match internal metrics and diagnostics
events.

The `lookup` operator no longer returns events for which none of the provided
fields exist.
