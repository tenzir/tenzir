---
title: "Incorrect values when charting"
type: bugfix
author: raxyte
created: 2025-06-13T17:18:09Z
pr: 5281
---

The charting operators did not update aggregations correctly, which resulted in
out-of-sync or `null` values.
