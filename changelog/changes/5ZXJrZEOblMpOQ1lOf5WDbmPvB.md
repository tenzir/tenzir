---
title: Load "all" plugins by default & allow "empty" values
type: change
authors: Dakostu
pr: 2689
---

VAST now loads all plugins by default. To revert to the old behavior,
explicitly set the `vast.plugins` option to have no value.
