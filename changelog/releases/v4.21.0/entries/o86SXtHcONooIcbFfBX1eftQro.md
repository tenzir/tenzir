---
title: "Stop using connection timeout to get node components"
type: bugfix
author: dominiklohmann
created: 2024-09-16T13:28:44Z
pr: 4597
---

The `import` and `partitions` operators and the `tenzir-ctl rebuild` command no
longer occasionally fail with request timeouts when the node is under high load.
