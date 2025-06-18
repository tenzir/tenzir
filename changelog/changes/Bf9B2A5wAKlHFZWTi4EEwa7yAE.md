---
title: "Incorrect values when charting"
type: bugfix
authors: raxyte
pr: 5281
---

The charting operators did not update aggregations correctly, which resulted in
out-of-sync or `null` values.
