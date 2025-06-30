---
title: "Fixed shutdown hang during storage optimization"
type: bugfix
authors: IyeOnline
pr: 5301
---

Nodes periodically merge and optimize their storage over time. We fixed a hang
on shutdown for nodes while this process was ongoing.
