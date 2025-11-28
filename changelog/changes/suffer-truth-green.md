---
title: "Fixed pubsub back-pressure"
type: bugfix
authors: IyeOnline
pr: 5587
---

We fixed a bug in the back-pressure propagation across `publish` and `subscribe`,
that could cause pipelines to stall.
