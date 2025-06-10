---
title: "Implement strides for the `slice` operator"
type: feature
authors: dominiklohmann
pr: 4216
---

The `slice` operator now supports strides in the form of `slice
<begin>:<end>:<stride>`. Negative strides reverse the event order. The new
`reverse` operator is a short form of `slice ::-1` and reverses the event order.
