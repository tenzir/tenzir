---
title: "Fixed assertion failure in `to_hive`"
type: bugfix
author: IyeOnline
created: 2025-11-24T19:05:55Z
pr: 5582
---

We addressed an assertion failure in the `to_hive` operator, which could fail
the partitioned writes for some data and format combinations.
