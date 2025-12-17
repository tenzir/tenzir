---
title: "Respect the default fp-rate setting"
type: bugfix
author: dominiklohmann
created: 2022-06-03T15:42:12Z
pr: 2325
---

VAST now reads the default false-positive rate for sketches correctly. This
broke accidentally with the v2.0 release. The option moved from
`vast.catalog-fp-rate` to `vast.index.default-fp-rate`.
