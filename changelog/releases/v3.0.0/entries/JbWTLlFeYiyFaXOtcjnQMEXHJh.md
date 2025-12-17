---
title: "Prevent query_processor from hanging when there are no candidate partitions"
type: bugfix
author: Dakostu
created: 2023-02-08T18:50:00Z
pr: 2924
---

The VAST client will now terminate properly when using the `count` command with
a query which delivers zero results.
