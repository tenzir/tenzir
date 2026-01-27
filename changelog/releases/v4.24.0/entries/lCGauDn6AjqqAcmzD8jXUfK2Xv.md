---
title: "Fix crash in `context::enrich` for heterogeneous enrichments"
type: feature
author: dominiklohmann
created: 2024-12-02T16:52:56Z
pr: 4828
---

The `network` function returns the network address of a CIDR subnet. For
example, `192.168.0.0/16.network()` returns `192.168.0.0`.
