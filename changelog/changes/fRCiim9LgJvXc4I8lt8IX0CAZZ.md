---
title: "Introduce `select` / `replace` / `extend` operators"
type: change
authors: dominiklohmann
pr: 2423
---

The `put` pipeline operator is now called `select`, as we've abandoned plans to
integrate the functionality of `replace` into it.

The `replace` pipeline operator now supports multiple replacements in one
configuration, which aligns the behavior with other operators.
