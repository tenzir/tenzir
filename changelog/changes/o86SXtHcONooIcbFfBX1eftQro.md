---
title: "Stop using connection timeout to get node components"
type: bugfix
authors: dominiklohmann
pr: 4597
---

The `import` and `partitions` operators and the `tenzir-ctl rebuild` command no
longer occasionally fail with request timeouts when the node is under high load.
