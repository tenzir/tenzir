---
title: "Make feather stores read incrementally"
type: feature
author: dominiklohmann
created: 2022-12-19T17:05:00Z
pr: 2805
---

VAST's Feather store now yields initial results much faster and performs better
when running queries affecting a large number of partitions by doing smaller
incremental disk reads as needed rather than one large disk read upfront.
