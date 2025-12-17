---
title: "Fix response promises for disk monitor deletion"
type: bugfix
author: dominiklohmann
created: 2021-09-29T12:40:17Z
pr: 1892
---

The disk monitor no longer fails to delete segments of particularly busy
partitions with the `segment-store` store backend.
