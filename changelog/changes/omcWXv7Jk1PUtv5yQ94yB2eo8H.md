---
title: "Add an operator for partition candidate checks"
type: feature
authors: dominiklohmann
pr: 4329
---

The `partitions [<expr>]` source operator supersedes `show partitions` (now
deprecated) and supports an optional expression as a positional argument for
showing only the partitions that would be considered in `export | where <expr>`.
