---
title: "Normalize expressions for live and unpersisted data"
type: bugfix
authors: tobim
pr: 4774
---

We fixed a crash in pipelines that use the `export` operator and a subsequent
`where` filter with certain expressions.
