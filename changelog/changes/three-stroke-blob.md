---
title: "`frequency` aggregation function"
type: feature
authors: dominiklohmann
pr: 5346
---

We added the `frequency` aggregation function that calculates the relative
frequency distribution of grouped values. The function returns a list of records
containing each unique value and its frequency as a decimal between 0 and 1,
sorted by frequency in descending order. Null values are excluded from the
calculation.
