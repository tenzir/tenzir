---
title: "Fix response promises for disk monitor deletion"
type: bugfix
authors: dominiklohmann
pr: 1892
---

The disk monitor no longer fails to delete segments of particularly busy
partitions with the `segment-store` store backend.
