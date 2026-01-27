---
title: "Pipeline activity refresh without running pipelines"
type: bugfix
author: jachris
created: 2025-06-12T15:31:16Z
pr: 5278
---

The `pipeline::activity` operator now always yields new events, even when all
running pipelines are hidden.
