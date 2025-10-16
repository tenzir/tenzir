---
title: "Improved pipeline execution"
type: change
authors: jachris
pr: 5519
---

We fine-tuned the scheduling logic responsible for the execution of pipelines.
In particular, certain pipelines that invoke parsing functions now take
significantly less memory to run.
