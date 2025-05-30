---
title: "Fix verification of large FlatBuffers tables"
type: bugfix
authors: dominiklohmann
pr: 4137
---

Lookup tables with more than 1M entries failed to load after the node was
restarted. This no longer happens.
