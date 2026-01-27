---
title: "Fix verification of large FlatBuffers tables"
type: bugfix
author: dominiklohmann
created: 2024-04-22T13:03:05Z
pr: 4137
---

Lookup tables with more than 1M entries failed to load after the node was
restarted. This no longer happens.
