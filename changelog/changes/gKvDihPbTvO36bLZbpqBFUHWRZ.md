---
title: Revert "Fix potential race condition between evaluator and partition"
type: bugfix
authors: lava
pr: 1381
---

An ordering issue introduced in
[#1295](https://github.com/tenzir/vast/pull/1295) that could lead to a segfault
with long-running queries was reverted.
