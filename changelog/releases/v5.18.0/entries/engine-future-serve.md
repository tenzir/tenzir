---
title: "Improved pipeline execution"
type: change
author: jachris
created: 2025-12-17T09:23:13Z
pr: [5519, 5525]
---

We fine-tuned the scheduling logic responsible for the execution of pipelines.
In particular, certain pipelines that invoke parsing functions now take
significantly less memory to run. Furthermore, `if` runs much faster in
situations with many small batches, preventing pipeline congestion and therefore
also lower memory usage.
