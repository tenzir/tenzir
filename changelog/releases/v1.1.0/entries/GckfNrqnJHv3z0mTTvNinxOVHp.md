---
title: "Send initial db state to new partition creation listeners"
type: bugfix
author: lava
created: 2022-02-22T13:57:03Z
pr: 2103
---

We fixed a bug that potentially resulted in the wrong subset of partitions to be
considered during query evaluation.
