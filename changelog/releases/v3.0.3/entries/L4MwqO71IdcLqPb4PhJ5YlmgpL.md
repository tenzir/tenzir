---
title: "PRs 3004-3010"
type: feature
author: dominiklohmann
created: 2023-03-16T11:55:16Z
pr: 3004
---

The new `vast exec` command executes a pipeline locally. It takes a single
argument representing a closed pipeline, and immediately executes it. This is
the foundation for a new, pipeline-first VAST, in which most operations are
expressed as pipelines.
