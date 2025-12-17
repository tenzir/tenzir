---
title: "Make `feather` the default store-backend"
type: change
author: dominiklohmann
created: 2022-09-23T10:30:49Z
pr: 2587
---

The default store backend of VAST is now `feather`. Reading from VAST's custom
`segment-store` backend is still transparently supported, but new partitions
automatically write to the Apache Feather V2 backend instead.
