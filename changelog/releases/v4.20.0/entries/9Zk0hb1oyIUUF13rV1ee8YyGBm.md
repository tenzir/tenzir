---
title: "Perform individual catalog lookups in `lookup`"
type: feature
author: dominiklohmann
created: 2024-08-30T09:12:30Z
pr: 4535
---

The `lookup` operator is now smarter about retroactive lookups for frequently
updated contexts and avoids loading data from disk multiple times for context
updates that arrive shortly after one another.
