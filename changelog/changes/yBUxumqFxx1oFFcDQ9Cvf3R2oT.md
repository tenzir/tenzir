---
title: "Implement optional dense indexes"
type: feature
authors: patszt
pr: 2430
---

VAST's partition indexes are now optional, allowing operators to control the
trade-off between disk-usage and query performance for every field.
