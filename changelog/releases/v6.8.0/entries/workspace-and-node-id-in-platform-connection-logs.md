---
title: Workspace and node id in platform connection logs
type: change
authors:
  - lava
prs:
  - 6450
created: 2026-07-15T14:11:30.97802Z
---

The log message emitted when a node connects to the Tenzir Platform now names
the workspace it authenticated into. Nodes that self-register with a workspace
registration key additionally print their own node id:

```
node connected to platform via wss://ws.tenzir.app:443/production into workspace t-abcd1234 as node ne-1a2b3c4d
```
