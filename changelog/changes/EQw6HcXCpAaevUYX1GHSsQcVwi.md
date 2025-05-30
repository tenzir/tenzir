---
title: "Add active partition actor to unpersisted partitions on decomission"
type: bugfix
authors: patszt
pr: 2500
---

VAST no longer occasionally prints warnings about no longer available partitions
when queries run concurrently to imports.
