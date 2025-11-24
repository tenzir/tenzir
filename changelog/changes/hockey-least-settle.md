---
title: Fixed assertion failure in `to_hive`
type: bugfix
authors: IyeOnline
pr: 5582
---

We addressed an assertion failure in the `to_hive` operator, which could fail
the partitioned writes for some data and format combinations.
