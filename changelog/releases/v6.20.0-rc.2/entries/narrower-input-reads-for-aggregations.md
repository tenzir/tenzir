---
title: Narrower input reads for aggregations
type: change
authors:
  - mavam
created: 2026-10-06T08:15:34.512557Z
---

`summarize` now tells upstream operators which fields it needs, so sources that support projections, such as `read_parquet`, read only the grouping keys and aggregate arguments instead of entire events. This also applies to `top` and `rare` and to `summarize` inside subpipelines, such as in `group`. A `summarize` with `emit`, `mode`, or `output` options still reads entire events.
