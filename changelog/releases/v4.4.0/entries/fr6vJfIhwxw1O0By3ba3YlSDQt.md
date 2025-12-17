---
title: "Update the plugins submodule pointer to include extended & serialized pipeline states"
type: feature
author: Dakostu
created: 2023-10-16T07:50:26Z
pr: 3554
---

The new `completed` pipeline state in the pipeline manager shows when a
pipeline has finished execution.

If the node with running pipelines crashes, they will be marked as `failed`
upon restarting.
