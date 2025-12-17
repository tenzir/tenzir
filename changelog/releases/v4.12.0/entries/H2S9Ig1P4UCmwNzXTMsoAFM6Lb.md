---
title: "Remove metrics from `/pipeline/list`"
type: change
author: dominiklohmann
created: 2024-04-12T12:27:38Z
pr: 4114
---

The `show pipelines` operator and `/pipeline/list` endpoint no longer include
pipeline metrics. We recommend using the `metrics` operator instead, which
offers the same data in a more flexible way.
