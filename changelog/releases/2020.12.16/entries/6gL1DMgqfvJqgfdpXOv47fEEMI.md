---
title: "Make Zeek writer work with all data types"
type: change
author: mavam
created: 2020-12-08T13:48:09Z
pr: 1205
---

The `zeek` export format now strips off the prefix `zeek.` to ensure full
compatibility with regular Zeek output. For all non-Zeek types, the prefix
remains intact.
