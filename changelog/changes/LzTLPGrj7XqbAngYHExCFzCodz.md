---
title: "Introduce a potpourri of smaller improvements"
type: change
authors: dominiklohmann
pr: 2832
---

VAST now ignores the previously deprecated options `vast.meta-index-fp-rate`,
`vast.catalog-fp-rate`, `vast.transforms` and `vast.transform-triggers`.
Similarly, setting `vast.store-backend` to `segment-store` now results in an
error rather than a graceful fallback to the default store.
