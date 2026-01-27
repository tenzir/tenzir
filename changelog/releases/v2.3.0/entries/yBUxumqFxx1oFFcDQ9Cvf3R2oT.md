---
title: "Implement optional dense indexes"
type: feature
author: patszt
created: 2022-08-08T12:42:25Z
pr: 2430
---

VAST's partition indexes are now optional, allowing operators to control the
trade-off between disk-usage and query performance for every field.
