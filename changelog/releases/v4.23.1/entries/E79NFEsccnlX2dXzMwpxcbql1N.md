---
title: "Normalize expressions for live and unpersisted data"
type: bugfix
author: tobim
created: 2024-11-15T12:57:50Z
pr: 4774
---

We fixed a crash in pipelines that use the `export` operator and a subsequent
`where` filter with certain expressions.
