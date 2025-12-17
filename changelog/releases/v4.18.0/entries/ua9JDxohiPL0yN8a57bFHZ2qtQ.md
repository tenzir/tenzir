---
title: "Revamp the `export` operator"
type: change
author: dominiklohmann
created: 2024-07-10T08:37:27Z
pr: 4365
---

The previously deprecated `--low-priority` option for the `export` operator no
longer exists. The new `--parallel <level>` option allows tuning how many
worker threads the operator uses at most for querying persisted events.
