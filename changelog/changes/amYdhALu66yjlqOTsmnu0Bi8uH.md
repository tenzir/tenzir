---
title: "Simplify id space management"
type: bugfix
authors: tobim
pr: 908
---

A bogus import process that assembled table slices with a greater number of
events than expected by the node was able to lead to wrong query results.
