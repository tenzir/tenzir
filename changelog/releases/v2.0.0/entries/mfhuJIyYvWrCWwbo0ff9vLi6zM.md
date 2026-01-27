---
title: "Rename `meta-index` to `catalog`"
type: change
author: dominiklohmann
created: 2022-03-03T11:13:49Z
pr: 2128
---

The `meta-index` is now called the `catalog`. This affects multiple metrics and
entries in the output of `vast status`, and the configuration option
`vast.meta-index-fp-rate`, which is now called `vast.catalog-fp-rate`.
