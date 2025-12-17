---
title: "Rename statistics event to metrics"
type: change
author: dominiklohmann
created: 2020-05-13T14:12:55Z
pr: 870
---

The command line flag for disabling the accountant has been renamed to
`--disable-metrics` to more accurately reflect its intended purpose. The
internal `vast.statistics` event has been renamed to `vast.metrics`.
