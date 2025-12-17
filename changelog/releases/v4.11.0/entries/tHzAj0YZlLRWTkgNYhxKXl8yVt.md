---
title: "Fix invalid assertion in `compress` operator"
type: bugfix
author: dominiklohmann
created: 2024-03-18T15:51:33Z
pr: 4048
---

The `compress` and `to` operators no longer fail when compression is unable to
further reduce the size of a batch of bytes.
