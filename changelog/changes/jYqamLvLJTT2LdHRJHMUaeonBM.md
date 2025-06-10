---
title: "Fix potential race condition between evaluator and partition"
type: bugfix
authors: lava
pr: 1295
---

A potential race condition that could lead to a hanging export if a partition
was persisted just as it was scanned no longer exists.
