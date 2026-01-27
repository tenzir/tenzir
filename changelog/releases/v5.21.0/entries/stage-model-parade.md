---
title: "Fixed a race in concurrent function application"
type: bugfix
author: tobim
created: 2025-11-19T13:38:10Z
pr: 5578
---

We fixed a race condition in case multiple pipelines used the same
transformation on the same data, such as string manipulations in where clauses
to filter the input. The bug caused those pipelines to fail sporadically.
