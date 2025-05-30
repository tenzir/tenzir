---
title: "Make feather stores read incrementally"
type: feature
authors: dominiklohmann
pr: 2805
---

VAST's Feather store now yields initial results much faster and performs better
when running queries affecting a large number of partitions by doing smaller
incremental disk reads as needed rather than one large disk read upfront.
