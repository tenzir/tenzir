---
title: "Expose pipeline operator metrics in execution node and pipeline executor"
type: feature
authors: Dakostu
pr: 3376
---

Pipeline metrics (total ingress/egress amount and average rate per second) are
now visible in the `pipeline-manager`, via the `metrics` field in the
`/pipeline/list` endpoint result.
