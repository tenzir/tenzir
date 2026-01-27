---
title: "Fix shutdown of connected pipelines alongside node"
type: bugfix
author: dominiklohmann
created: 2024-04-05T07:20:25Z
pr: 4093
---

Pipelines run with the `tenzir` binary that connected to a Tenzir Node did
sometimes not shut down correctly when the node shut down. This now happens
reliably.
