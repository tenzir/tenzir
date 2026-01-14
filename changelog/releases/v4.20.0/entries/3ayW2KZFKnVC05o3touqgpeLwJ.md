---
title: "Remove the legacy metrics system"
type: change
author: dominiklohmann
created: 2024-08-21T20:29:43Z
pr: 4381
---

The previously deprecated legacy metrics system configured via the
`tenzir.metrics` configuration section no longer exists. Use the `metrics`
operator instead.

`lookup` metrics no longer contain the `snapshot` field; instead, the values
show in the `retro` field.
