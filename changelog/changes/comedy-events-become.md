---
title: "`every` and `cron` subpipelines"
type: change
authors: raxyte
pr: 5410
---

We changed the execution model for `every` and `cron` subpipelines, resulting
in:
- operators such as `context::load` now execute properly.
- subpipelines can contain both `remote` and `local` operators.
- subpipelines must not accept or output bytes.
