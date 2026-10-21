---
title: Parallel load balancing
type: feature
authors:
  - aljazerzen
created: 2026-10-13T09:07:09.95309Z
---

`load_balance` now distributes targets across parallel workers while keeping one pipeline per target. For example: `parallel 2 { load_balance $targets { to_tcp $targets { write_json } } }`.
