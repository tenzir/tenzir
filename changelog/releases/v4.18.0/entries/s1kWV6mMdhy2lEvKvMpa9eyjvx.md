---
title: "Push expressions into `subscribe` for better metrics"
type: change
author: dominiklohmann
created: 2024-07-04T08:04:27Z
pr: 4349
---

Pipeline activity for pipelines starting with `subscribe | where <expr>` will no
longer report ingress that does not match the provided filter expression.
