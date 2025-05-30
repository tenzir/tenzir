---
title: "Fix a rare crash in the index actor on startup"
type: bugfix
authors: dominiklohmann
pr: 4846
---

We fixed a rare crash on startup that would occur when starting the
`tenzir-node` process was so slow that it would try to emit metrics before the
component handling metrics was ready.
