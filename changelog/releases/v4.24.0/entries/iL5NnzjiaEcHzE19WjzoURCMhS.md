---
title: "Port Contexts to TQL2"
type: bugfix
author: dominiklohmann
created: 2024-11-14T10:03:09Z
pr: 4753
---

The last metric emitted for each run of the `enrich` operator was incorrectly
named `tenzir.enrich.metrics` instead of `tenzir.metrics.enrich`, causing it not
to be available via `metrics enrich`.
