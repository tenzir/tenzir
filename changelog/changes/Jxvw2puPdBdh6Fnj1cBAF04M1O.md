---
title: "Always store pipeline state for configured packaged pipelines"
type: bugfix
authors: lava
pr: 4479
---

Pipelines from packages now correctly remember their last run number and last
state when the reinstalling the package.
