---
title: "Prevent query_processor from hanging when there are no candidate partitions"
type: bugfix
authors: Dakostu
pr: 2924
---

The VAST client will now terminate properly when using the `count` command with
a query which delivers zero results.
