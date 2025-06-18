---
title: "Update the main repository to include the pipeline manager autostart changes"
type: feature
authors: Dakostu
pr: 3785
---

Pipeline states in the `/pipeline` API will not change upon node shutdown
anymore. When a node restarts afterwards, previously running pipelines will
continue to run while paused pipelines will load in a stopped state.
