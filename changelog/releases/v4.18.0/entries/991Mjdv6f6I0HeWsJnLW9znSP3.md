---
title: "Fix open partition tracking in the `lookup` operator"
type: bugfix
author: dominiklohmann
created: 2024-07-09T07:48:00Z
pr: 4363
---

We fixed a rare bug that caused the `lookup` operator to exit unexpectedly when
using a high value for the operator's `--parallel` option.
