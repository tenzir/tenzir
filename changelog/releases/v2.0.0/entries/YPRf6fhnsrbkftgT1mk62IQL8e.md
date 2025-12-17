---
title: "Fix query pruning in the catalog"
type: bugfix
author: lava
created: 2022-05-09T14:13:23Z
pr: 2264
---

The query optimizer incorrectly transformed queries with conjunctions or
disjunctions with several operands testing against the same string value,
leading to missing result. This was rarely an issue in practice before the
introduction of homogenous partitions with the v2.0 release.
