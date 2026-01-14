---
title: "Perform individual catalog lookups in `lookup`"
type: bugfix
author: dominiklohmann
created: 2024-08-30T09:12:30Z
pr: 4535
---

We fixed a bug that sometimes caused the `retro.queued_events` value in `lookup`
metrics to stop going down again.
