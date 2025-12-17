---
title: "Subscribe to multiple topics at once"
type: feature
author: jachris
created: 2025-09-26T10:49:41Z
pr: 5494
---

The `subscribe` operator now accepts multiple topics to subscribe to. For
example, `subscribe "notices", "alerts"` subscribes to both the `notices`, and
the `alerts` topic. This makes it easier to build pipelines that join multiple
topics back together.
