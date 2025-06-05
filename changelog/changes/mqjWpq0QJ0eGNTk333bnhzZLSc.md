---
title: "Add running and paused times to pipeline metrics"
type: feature
authors: dominiklohmann
pr: 3940
---

Operator metrics now separately track the time that an operator was paused or
running in the `time_paused` and `time_running` values in addition to the
wall-clock time in `time_total`. Throughput rates now exclude the paused time
from their calculation.
