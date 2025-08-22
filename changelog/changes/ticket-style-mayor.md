---
title: "Buffering in the `fork` operator"
type: bugfix
authors: raxyte
pr: 5436
---

We fixed an issue in the `fork` operator where the last event would get stuck.
