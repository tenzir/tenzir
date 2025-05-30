---
title: "Rename statistics event to metrics"
type: change
authors: dominiklohmann
pr: 870
---

The command line flag for disabling the accountant has been renamed to
`--disable-metrics` to more accurately reflect its intended purpose. The
internal `vast.statistics` event has been renamed to `vast.metrics`.
