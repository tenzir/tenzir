---
title: "Add statistical aggregation functions"
type: feature
author: dominiklohmann
created: 2024-05-17T08:26:59Z
pr: 4208
---

The new `mean` aggregation function computes the mean of grouped numeric values.

The new `approximate_median` aggregation function computes an approximate median
of grouped numeric values using the t-digest algorithm.

The new `stddev` and `variance` aggregation functions compute the standard
deviation and variance of grouped numeric values, respectively.

The new `collect` aggregation function collects a list of all non-null grouped
values. Unlike `distinct`, this function does not remove dulicates and the
results may appear in any order.
