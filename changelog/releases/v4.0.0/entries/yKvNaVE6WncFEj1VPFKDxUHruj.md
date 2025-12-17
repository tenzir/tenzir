---
title: "Remove previously deprecated options"
type: change
author: dominiklohmann
created: 2023-07-13T16:00:30Z
pr: 3358
---

The previously deprecated options `tenzir.pipelines` (replaced with
`tenzir.operators`) and `tenzir.pipeline-triggers` (no replacement) no longer
exist.

The previously deprecated deprecated types `addr`, `count`, `int`, and `real`
(replaced with `ip`, `uint64`, `int64`, and `double`, respectively) no longer
exist.
