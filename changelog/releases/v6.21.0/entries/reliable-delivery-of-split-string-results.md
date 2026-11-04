---
title: Reliable delivery of split string results
type: bugfix
authors:
  - zedoraps
created: 2026-11-03T09:30:36.416425Z
---

Event batches containing results from `split()` or `split_regex()` now reach
subscribers reliably. Previously, some batches with repeated strings or removed
separators could produce no output after `publish`, or fail a
`write_bitz`/`read_bitz` round trip.
