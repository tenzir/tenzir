---
title: "Pipeline activity refresh without running pipelines"
type: bugfix
authors: jachris
pr: 5278
---

The `pipeline::activity` operator now always yields new events, even when all
running pipelines are hidden.
