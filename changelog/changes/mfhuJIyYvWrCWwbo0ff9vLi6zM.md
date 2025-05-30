---
title: "Rename `meta-index` to `catalog`"
type: change
authors: dominiklohmann
pr: 2128
---

The `meta-index` is now called the `catalog`. This affects multiple metrics and
entries in the output of `vast status`, and the configuration option
`vast.meta-index-fp-rate`, which is now called `vast.catalog-fp-rate`.
