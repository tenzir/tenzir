---
title: Fix crash when connecting to unresolvable host
type: bugfix
author: lava
pr: 5827
created: 2026-03-26T11:47:48.810386Z
---

Setting `TENZIR_ENDPOINT` to an unresolvable hostname no longer crashes the pipeline with a segfault.
