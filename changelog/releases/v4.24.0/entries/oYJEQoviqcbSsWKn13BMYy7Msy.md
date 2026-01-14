---
title: "Introduce a `zip` function for merging lists"
type: feature
author: dominiklohmann
created: 2024-12-03T14:19:34Z
pr: 4803
---

The `zip` function merges two lists into a single list of a record with two
fields `left` and `right`. For example, `zip([1, 2], [3, 4])` returns `[{left:
1, right: 3}, {left: 2, right: 4}]`.
