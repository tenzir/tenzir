---
title: "Avoid idle wakeups in `load_tcp`"
type: bugfix
author: dominiklohmann
created: 2025-03-04T14:34:39Z
pr: 5035
---

We fixed a bug that caused unnecessary idle wakeups in the `load_tcp` operator,
throwing off scheduling of pipelines using it. Under rare circumstances, this
could also lead to partially duplicated output of the operator's nested
pipeline.
