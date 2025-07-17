---
title: "Add `hour`, `minute`, and `second` time component extraction"
type: feature
authors: dominiklohmann
pr: 5190
---

The `hour`, `minute`, and `second` functions extract the respective components
of a `time` value, and compliment the existing `year`, `month`, and `day`
functions. The `second` function includes subsecond precision.
