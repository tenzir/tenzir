---
title: "Expose the lower-level `load`, `parse`, `print`, and `save` operators"
type: feature
author: dominiklohmann
created: 2023-04-17T19:25:37Z
pr: 3079
---

The new `from <connector> [read <format>]`, `read <format> [from <connector>]`,
`write <format> [to <connector>]`, and `to <connector> [write <format>]`
operators bring together a connector and a format to prduce and consume events,
respectively. Their lower-level building blocks `load <connector>`, `parse
<format>`, `print <format>`, and `save <connector>` enable expert users to
operate on raw byte streams directly.
