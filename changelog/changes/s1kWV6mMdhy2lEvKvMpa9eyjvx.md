---
title: "Push expressions into `subscribe` for better metrics"
type: change
authors: dominiklohmann
pr: 4349
---

Pipeline activity for pipelines starting with `subscribe | where <expr>` will no
longer report ingress that does not match the provided filter expression.
