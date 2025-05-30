---
title: "Fix an off-by-one error when loading persisted contexts"
type: bugfix
authors: dominiklohmann
pr: 4045
---

We fixed a bug that caused every second context to become unavailable after a
restarting the node.
