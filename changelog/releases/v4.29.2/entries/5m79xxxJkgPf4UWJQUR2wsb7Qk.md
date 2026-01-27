---
title: "Make debugging `load_tcp` pipelines easier"
type: feature
author: dominiklohmann
created: 2025-03-06T18:30:01Z
pr: 5040
---

The newly added `max_buffered_chunks` for `load_tcp` controls how many reads
the operator schedules in advance on the socket. The option defaults to 10.
