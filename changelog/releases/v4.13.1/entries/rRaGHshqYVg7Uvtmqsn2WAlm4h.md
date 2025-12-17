---
title: "Remove stream managers when decommissioning partitions"
type: bugfix
author: dominiklohmann
created: 2024-05-13T13:24:30Z
pr: 4214
---

The node's CPU usage increased ever so slightly with every persisted partition,
eventually causing imports and exports to get stuck. This no longer happens.
