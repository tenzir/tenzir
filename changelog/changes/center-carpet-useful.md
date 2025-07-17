---
title: "Improved node robustness"
type: feature
authors: tobim
pr: 5233
---

Pipelines that are running in a node are now partially moved to a subprocess for
improved error resilience and resource utilization. Operators that need to
communicate with a component still run inside the main node process for
architectural reasons. You can set `tenzir.disable-pipeline-subprocesses: true`
in `tenzir.yaml` or `TENZIR_DISABLE_PIPELINE_SUBPROCESSES=true` on the command
line to opt out. This feature is enabled by default on Linux.
