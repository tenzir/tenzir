---
title: "Add active partition actor to unpersisted partitions on decomission"
type: bugfix
author: patszt
created: 2022-08-16T12:25:41Z
pr: 2500
---

VAST no longer occasionally prints warnings about no longer available partitions
when queries run concurrently to imports.
