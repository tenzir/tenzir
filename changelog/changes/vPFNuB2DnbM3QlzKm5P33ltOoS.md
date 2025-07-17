---
title: "Update the main repository to include the pipeline run ID"
type: feature
authors: Dakostu
pr: 3883
---

Managed pipelines now contain a new `total_runs` parameter that counts all
started runs. The new `run` field is available in the events delivered by the
`metrics` and `diagnostics` operators.
