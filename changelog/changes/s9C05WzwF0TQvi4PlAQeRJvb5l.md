---
title: "Add a few new aggregation functions for TQL2"
type: feature
authors: dominiklohmann
pr: 4679
---

We added three new, TQL2-exclusive aggregation functions: `first`, `last`, and
`mode`. The functions return the first, last, and most common non-null value per
group, respectively.
