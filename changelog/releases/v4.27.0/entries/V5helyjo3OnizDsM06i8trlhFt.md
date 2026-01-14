---
title: "Add `batches` to input/output operator metrics"
type: feature
author: dominiklohmann
created: 2025-01-30T10:33:20Z
pr: 4962
---

`metrics "operator"` now includes a new `batches` field under the `input` and
`output` records that counts how many batches of events or bytes the metric was
generated from.
