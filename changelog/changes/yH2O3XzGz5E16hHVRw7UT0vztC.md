---
title: "Fix incorrect context updates count in lookup metrics"
type: bugfix
authors: dominiklohmann
pr: 4655
---

We fixed a bug that caused the `context_updates` field in `metrics lookup` to be
reported once per field specified in the corresponding `lookup` operator instead
of being reported once per operator in total.
