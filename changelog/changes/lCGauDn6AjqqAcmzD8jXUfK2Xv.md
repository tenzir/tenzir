---
title: "Fix crash in `context::enrich` for heterogeneous enrichments"
type: feature
authors: dominiklohmann
pr: 4828
---

The `network` function returns the network address of a CIDR subnet. For
example, `192.168.0.0/16.network()` returns `192.168.0.0`.
