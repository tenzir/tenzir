---
title: "Introduce CAF metrics"
type: feature
authors: dominiklohmann
pr: 4897
---

`metrics "caf"` offers insights into Tenzir's underlying actor system. This is
primarily aimed at developers for performance benchmarking.

The new `merge` function combines two records. `merge(foo, bar)` is a shorthand
for `{...foo, ...bar}`.
