---
title: "Bump the tenzir-plugins submodule pointer to include the pipeline manager's resuming and pausing functionality"
type: feature
authors: Dakostu
pr: 3471
---

The `pause` action in the `/pipeline/update` endpoint suspends a pipeline and
sets its state to `paused`. Resume it with the `start` action.

Newly created pipelines are now in a new `created` rather than `stopped` state.
