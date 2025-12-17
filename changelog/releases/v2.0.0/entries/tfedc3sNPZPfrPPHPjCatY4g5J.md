---
title: "Implement support for transforms that apply to every type and use compaction for aging"
type: feature
author: lava
created: 2022-04-08T16:00:08Z
pr: 2186
---

VAST v1.0 deprecated the experimental aging feature. Given popular demand we've
decided to un-deprecate it, and to actually implement it on top of the same
building blocks the compaction mechanism uses. This means that it is now fully
working and no longer considered experimental.
