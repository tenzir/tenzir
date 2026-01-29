---
title: Targeted conversion functions for compile-time improvement
type: change
authors:
  - mavam
created: 2026-01-29T00:00:00Z
---

We added dedicated `convert(data, T)` overloads for `index_config`,
`concepts_map`, and `web::configuration` types. These targeted conversion
functions replace generic `match()` operations, reducing compile times
significantly without changing runtime behavior.
