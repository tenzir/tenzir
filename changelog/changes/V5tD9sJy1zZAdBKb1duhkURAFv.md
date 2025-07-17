---
title: "Prevent delays for blocking operators"
type: bugfix
authors: dominiklohmann
pr: 3743
---

Pipeline operators blocking in their execution sometimes caused results to be
delayed. This is no longer the case. This bug fix also reduces the time to first
result for pipelines with many operators.
