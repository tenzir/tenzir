---
title: "Prevent unbounded memory usage in `export --live`"
type: change
author: dominiklohmann
created: 2024-07-16T15:57:42Z
pr: 4396
---

`metrics export` now includes an additional field that shows the number of
queued events in the pipeline.
