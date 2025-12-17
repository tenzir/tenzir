---
title: "Push expressions into `subscribe` for better metrics"
type: bugfix
author: dominiklohmann
created: 2024-07-04T08:04:27Z
pr: 4349
---

Pipelines of the form `export --live | where <expr>` failed to filter with
type extractors or concepts. This now works as expected.
