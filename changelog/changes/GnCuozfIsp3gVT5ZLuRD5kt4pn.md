---
title: "Implement `strict { ... }`"
type: feature
authors: raxyte
pr: 5174
---

We added a new `strict` operator that takes a pipeline and treats all warnings
emitted by that pipeline as errors, i.e., effectively stopping the pipeline at
the first diagnostic. This is useful when you to ensure want a critical piece of
your pipeline does not continue in unexpected scenarios.
