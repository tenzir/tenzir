---
title: "Allow disabling node-to-node connections"
type: feature
author: dominiklohmann
created: 2024-07-11T16:11:35Z
pr: 4380
---

Setting the `tenzir.endpoint` option to `false` now causes the node not to
listen for node-to-node connections. Previously, the port was always exposed for
other nodes or `tenzir` processes to connect.
