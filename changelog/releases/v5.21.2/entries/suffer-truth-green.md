---
title: "Fixed pubsub back-pressure"
type: bugfix
author: IyeOnline
created: 2025-11-28T16:35:33Z
pr: 5587
---

We fixed a bug in the back-pressure propagation across `publish` and `subscribe`,
that could cause pipelines to stall.
