---
title: "Transform `read` and `write` into `parse` and `print`"
type: change
author: jachris
created: 2023-07-18T16:15:51Z
pr: 3365
---

The `parse` and `print` operators have been renamed to `read` and `write`,
respectively. The `read ... [from ...]` and `write ... [to ...]` operators
are not available anymore. If you did not specify a connector, you can
continue using `read ...` and `write ...` in many cases. Otherwise, use
`from ... [read ...]` and `to ... [write ...]` instead.
