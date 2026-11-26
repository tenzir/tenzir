---
title: Filters move past if and match when every branch keeps them
type: change
authors:
  - mavam
created: 2026-11-26T11:06:20.596638Z
---

A `where` after an `if` or `match` now moves above it when every branch keeps the same filter, even if the condition is expensive or the `match` dispatches on a computed value. This lets filters on common fields, such as a host name, reach the source through OCSF mappings that branch per event type.
