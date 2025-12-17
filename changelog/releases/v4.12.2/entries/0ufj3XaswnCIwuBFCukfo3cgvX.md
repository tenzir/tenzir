---
title: "Use consistent attributes for y-axis in `chart`"
type: bugfix
author: dominiklohmann
created: 2024-04-30T12:35:32Z
pr: 4173
---

The `chart` operator failed to render a chart when the y-axis was not specified
explicitly and the events contained more than two top-level fields. This no
longer happens.
