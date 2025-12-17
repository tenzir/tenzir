---
title: "Multiple node shutdown fixes"
type: bugfix
author: tobim
created: 2021-04-21T18:30:03Z
pr: 1563
---

The shutdown logic contained a bug that would make the node fail to terminate
in case a plugin actor is registered at said node.

A race condition in the shutdown logic that caused an assertion was fixed.
