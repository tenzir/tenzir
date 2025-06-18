---
title: "Make transform application transactional"
type: bugfix
authors: lava
pr: 2465
---

We fixed a race condition when VAST crashed while applying a partition
transform, leading to data duplication.
