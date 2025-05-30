---
title: Introduce `metrics "pipeline"`
type: change
authors: dominiklohmann
pr: 5024
---

`metrics "operator"` is now deprecated. Use `metrics "pipeline"` instead, which
offers a pre-aggregated view of pipeline metrics. We plan to remove operator
metrics in an upcoming release, as they are too expensive in large-scale
deployments.
