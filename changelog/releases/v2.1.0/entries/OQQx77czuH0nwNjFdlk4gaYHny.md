---
title: "Fix crash in query evaluation for new partitions"
type: bugfix
author: dominiklohmann
created: 2022-05-23T10:44:50Z
pr: 2295
---

VAST no longer crashes when a query arrives at a newly created active partition
in the time window between the partition creation and the first event arriving
at the partition.
