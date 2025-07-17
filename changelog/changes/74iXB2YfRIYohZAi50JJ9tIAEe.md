---
title: "Count nulls and absent values in `top` and `rare`"
type: bugfix
authors: dominiklohmann
pr: 3990
---

The `top` and `rare` operators now correctly count null and absent values.
Previously, they emitted a single event with a count of zero when any null or
absent values were included in the input.
