---
title: "Send initial db state to new partition creation listeners"
type: bugfix
authors: lava
pr: 2103
---

We fixed a bug that potentially resulted in the wrong subset of partitions to be
considered during query evaluation.
