---
title: "Fix an off-by-one error when loading persisted contexts"
type: bugfix
author: dominiklohmann
created: 2024-03-17T11:13:00Z
pr: 4045
---

We fixed a bug that caused every second context to become unavailable after a
restarting the node.
