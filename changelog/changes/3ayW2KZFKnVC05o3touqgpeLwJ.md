---
title: "Remove the legacy metrics system"
type: change
authors: dominiklohmann
pr: 4381
---

The previously deprecated legacy metrics system configured via the
`tenzir.metrics` configuration section no longer exists. Use the `metrics`
operator instead.

`lookup` metrics no longer contain the `snapshot` field; instead, the values
show in the `retro` field.
