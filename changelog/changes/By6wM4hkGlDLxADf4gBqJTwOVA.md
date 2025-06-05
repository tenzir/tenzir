---
title: "Improve the `export` operator"
type: feature
authors: dominiklohmann
pr: 3909
---

The `export` operator gained a `--low-priority` option, which causes it to
interfere less with regular priority exports at the cost of potentially running
slower.
