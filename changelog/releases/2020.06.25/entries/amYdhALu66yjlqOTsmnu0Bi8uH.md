---
title: "Simplify id space management"
type: bugfix
author: tobim
created: 2020-06-16T08:32:10Z
pr: 908
---

A bogus import process that assembled table slices with a greater number of
events than expected by the node was able to lead to wrong query results.
