---
title: "Fix open partition tracking in the `lookup` operator"
type: bugfix
authors: dominiklohmann
pr: 4363
---

We fixed a rare bug that caused the `lookup` operator to exit unexpectedly when
using a high value for the operator's `--parallel` option.
