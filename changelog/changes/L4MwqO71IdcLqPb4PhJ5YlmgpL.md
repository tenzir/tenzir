---
title: "PRs 3004-3010"
type: feature
authors: dominiklohmann
pr: 3004
---

The new `vast exec` command executes a pipeline locally. It takes a single
argument representing a closed pipeline, and immediately executes it. This is
the foundation for a new, pipeline-first VAST, in which most operations are
expressed as pipelines.
