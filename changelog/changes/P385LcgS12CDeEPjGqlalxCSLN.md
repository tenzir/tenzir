---
title: "Start the telemetry loop of the index correctly"
type: bugfix
authors: dominiklohmann
pr: 2032
---

The index now emits the metrics `query.backlog.{low,normal}` and
`query.workers.{idle,busy}` reliably.
