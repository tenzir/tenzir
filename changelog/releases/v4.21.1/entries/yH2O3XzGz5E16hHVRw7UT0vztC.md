---
title: "Fix incorrect context updates count in lookup metrics"
type: bugfix
author: dominiklohmann
created: 2024-10-10T08:32:20Z
pr: 4655
---

We fixed a bug that caused the `context_updates` field in `metrics lookup` to be
reported once per field specified in the corresponding `lookup` operator instead
of being reported once per operator in total.
