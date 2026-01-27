---
title: "Start the telemetry loop of the index correctly"
type: bugfix
author: dominiklohmann
created: 2022-01-14T14:11:01Z
pr: 2032
---

The index now emits the metrics `query.backlog.{low,normal}` and
`query.workers.{idle,busy}` reliably.
