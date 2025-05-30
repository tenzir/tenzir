---
title: "Use `load -` and `read json` as implicit sources"
type: feature
authors: dominiklohmann
pr: 3329
---

Pipelines executed locally with `tenzir` now use `load -` and `read json` as
implicit sources. This complements `save -` and `write json --pretty` as
implicit sinks.
