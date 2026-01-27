---
title: "Introduce a potpourri of smaller improvements"
type: change
author: dominiklohmann
created: 2023-01-06T02:16:56Z
pr: 2832
---

VAST now ignores the previously deprecated options `vast.meta-index-fp-rate`,
`vast.catalog-fp-rate`, `vast.transforms` and `vast.transform-triggers`.
Similarly, setting `vast.store-backend` to `segment-store` now results in an
error rather than a graceful fallback to the default store.
