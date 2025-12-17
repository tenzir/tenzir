---
title: "Partition transforms"
type: feature
author: lava
created: 2021-10-25T19:09:51Z
pr: 1887
---

A new 'apply' handler in the index gives plugin authors the ability to
apply transforms over entire partitions. Previously, transforms were
limited to streams of table slice during import or export.
