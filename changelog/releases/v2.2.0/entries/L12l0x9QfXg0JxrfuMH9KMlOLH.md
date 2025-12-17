---
title: "Make transform application transactional"
type: bugfix
author: lava
created: 2022-07-28T18:31:44Z
pr: 2465
---

We fixed a race condition when VAST crashed while applying a partition
transform, leading to data duplication.
