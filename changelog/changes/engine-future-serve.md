---
title: "Improved pipeline execution"
type: change
authors: jachris
pr: [5519, 5525]
---

We fine-tuned the scheduling logic responsible for the execution of pipelines.
In particular, certain pipelines that invoke parsing functions now take
significantly less memory to run. Furthermore, `if` runs much faster in
situations with many small batches, preventing pipeline congestion and therefore
also lower memory usage.
