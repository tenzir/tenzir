---
title: "Prevent unbounded memory usage in `export --live`"
type: change
authors: dominiklohmann
pr: 4396
---

`metrics export` now includes an additional field that shows the number of
queued events in the pipeline.
