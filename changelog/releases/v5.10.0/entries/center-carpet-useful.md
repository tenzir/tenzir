---
title: "Improved node robustness"
type: feature
author: tobim
created: 2025-07-15T10:28:39Z
pr: 5233
---

We added an experimental feature to run node-independent operators of a pipeline
in dedicated subprocesses. This brings improved error resilience and resource
utilization. You can opt-in to this feature with the setting
`tenzir.disable-pipeline-subprocesses: false` in `tenzir.yaml`. We plan to
enable this feature by default in the future.
