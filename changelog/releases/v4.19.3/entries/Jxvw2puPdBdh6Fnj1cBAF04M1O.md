---
title: "Always store pipeline state for configured packaged pipelines"
type: bugfix
author: lava
created: 2024-08-06T17:21:03Z
pr: 4479
---

Pipelines from packages now correctly remember their last run number and last
state when the reinstalling the package.
