---
title: "Perform individual catalog lookups in `lookup`"
type: bugfix
authors: dominiklohmann
pr: 4535
---

We fixed a bug that sometimes caused the `retro.queued_events` value in `lookup`
metrics to stop going down again.
