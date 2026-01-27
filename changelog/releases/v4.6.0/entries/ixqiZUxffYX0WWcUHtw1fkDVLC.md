---
title: "Consider discard, export, and import as internal operators"
type: change
author: dominiklohmann
created: 2023-11-21T19:49:06Z
pr: 3658
---

Ingress and egress metrics for pipelines now indicate whether the pipeline
sent/received events to/from outside of the node with a new `internal` flag. For
example, when using the `export` operator, data is entering the pipeline from
within the node, so its ingress is considered internal.
