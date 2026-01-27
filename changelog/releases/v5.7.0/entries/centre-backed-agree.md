---
title: "Fixed shutdown hang during storage optimization"
type: bugfix
author: IyeOnline
created: 2025-06-30T12:07:24Z
pr: 5301
---

Nodes periodically merge and optimize their storage over time. We fixed a hang
on shutdown for nodes while this process was ongoing.
