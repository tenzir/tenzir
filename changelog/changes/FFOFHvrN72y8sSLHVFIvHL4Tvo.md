---
title: "Fix shutdown of connected pipelines alongside node"
type: bugfix
authors: dominiklohmann
pr: 4093
---

Pipelines run with the `tenzir` binary that connected to a Tenzir Node did
sometimes not shut down correctly when the node shut down. This now happens
reliably.
