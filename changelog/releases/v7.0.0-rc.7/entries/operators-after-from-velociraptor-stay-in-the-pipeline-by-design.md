---
title: Operators after from_velociraptor stay in the pipeline by design
type: change
authors:
  - mavam
created: 2026-11-11T16:08:14.412716Z
---

`from_velociraptor` now states in the optimized pipeline why it takes over none of the operators after it: a VQL query may have effects, such as collecting artifacts or subscribing to events, that a filter or limit in the query would change, and `max_rows` only sizes the response batches. The operators after it keep running in the pipeline as before.
