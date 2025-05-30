---
title: "Multiple node shutdown fixes"
type: bugfix
authors: tobim
pr: 1563
---

The shutdown logic contained a bug that would make the node fail to terminate
in case a plugin actor is registered at said node.

A race condition in the shutdown logic that caused an assertion was fixed.
