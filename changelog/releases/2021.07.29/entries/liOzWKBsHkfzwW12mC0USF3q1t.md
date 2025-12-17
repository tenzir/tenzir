---
title: "PRs 1720-1762-1802"
type: feature
author: lava
created: 2021-06-30T12:12:07Z
pr: 1720
---

VAST has new a `store_plugin` type for custom store backends that hold the raw
data of a partition. The new setting `vast.store-backend` controls the
selection of the store implementation, which has a default value is
`segment-store`. This is still an opt-in feature: unless the configuration
value is set, VAST defaults to the old implementation.
