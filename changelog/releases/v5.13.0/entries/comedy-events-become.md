---
title: "`every` and `cron` subpipelines"
type: change
author: raxyte
created: 2025-08-13T10:01:22Z
pr: 5410
---

We changed the execution model for `every` and `cron` subpipelines, resulting
in:
- operators such as `context::load` now execute properly.
- subpipelines can contain both `remote` and `local` operators.
- subpipelines must not accept or output bytes.
