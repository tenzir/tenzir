---
title: "Always shut down the pipeline manager first"
type: bugfix
authors: dominiklohmann
pr: 5163
---

We fixed a very rare bug that on shutdown could mark running pipelines as
stopped, completed, or failed, causing the pipelines not to restart alongside
the node.
