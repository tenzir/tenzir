---
title: "Bump the tenzir-plugins submodule pointer to include the pipeline manager's failure and rendered diagnostics functionality"
type: feature
author: Dakostu
created: 2023-08-31T08:11:25Z
pr: 3479
---

The `rendered` field in the pipeline manager diagnostics delivers a displayable
version of the diagnostic's error message.

Pipelines that encounter an error during execution are now in a new `failed`
rather than `stopped` state.
