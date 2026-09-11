---
title: Projection pushdown through nested pipelines
type: change
authors:
  - aljazerzen
created: 2026-09-10T16:04:22.515013Z
---

Tenzir now pushes field projections through branching operators and nested pipelines. Pipelines using `if`, `match`, `fork`, `fork_merge`, `merge`, `parallel`, `group`, `window`, `every`, `load_balance`, and `strict` can avoid reading fields that neither the main path nor any nested path needs, while retaining routing keys and other control-flow dependencies.
