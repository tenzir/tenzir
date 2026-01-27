---
title: "Load \"all\" plugins by default & allow \"empty\" values"
type: change
author: Dakostu
created: 2022-11-18T11:10:08Z
pr: 2689
---

VAST now loads all plugins by default. To revert to the old behavior,
explicitly set the `vast.plugins` option to have no value.
