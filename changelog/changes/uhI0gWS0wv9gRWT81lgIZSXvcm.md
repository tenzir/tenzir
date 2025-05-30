---
title: "Push expressions into `subscribe` for better metrics"
type: bugfix
authors: dominiklohmann
pr: 4349
---

Pipelines of the form `export --live | where <expr>` failed to filter with
type extractors or concepts. This now works as expected.
