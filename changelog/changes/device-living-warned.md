---
title: "Subscribe to multiple topics at once"
type: feature
authors: jachris
pr: 5494
---

The `subscribe` operator now accepts multiple topics to subscribe to. For
example, `subscribe "notices", "alerts"` subscribes to both the `notices`, and
the `alerts` topic. This makes it easier to build pipelines that join multiple
topics back together.
