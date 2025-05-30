---
title: "Make partition-local stores the default"
type: change
authors: lava
pr: 1876
---

The default store backend now is `segment-store` in order to enable the use of
partition transforms in the future. To continue using the (now deprecated)
legacy store backend, set `vast.store-backend` to archive.
