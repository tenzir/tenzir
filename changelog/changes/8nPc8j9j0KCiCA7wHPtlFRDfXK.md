---
title: "Introduce a query backlog in the index"
type: change
authors: tobim
pr: 1896
---

The `max-queries` configuration option now works at a coarser granularity. It
used to limit the number of queries that could simultaneously retrieve data,
but it now sets the number of queries that can be processed at the same time.
