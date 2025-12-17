---
title: "Tone down execution node backoff behavior"
type: bugfix
author: dominiklohmann
created: 2024-06-17T15:40:34Z
pr: 4297
---

We fixed a regression that caused excess CPU usage for some operators when idle.
This was most visible with the `subscribe`, `export`, `metrics`, `diagnostics`,
`lookup` and `enrich` operators.
