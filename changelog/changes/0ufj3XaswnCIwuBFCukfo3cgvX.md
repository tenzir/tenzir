---
title: "Use consistent attributes for y-axis in `chart`"
type: bugfix
authors: dominiklohmann
pr: 4173
---

The `chart` operator failed to render a chart when the y-axis was not specified
explicitly and the events contained more than two top-level fields. This no
longer happens.
