---
title: "Fix invalid assertion in `compress` operator"
type: bugfix
authors: dominiklohmann
pr: 4048
---

The `compress` and `to` operators no longer fail when compression is unable to
further reduce the size of a batch of bytes.
