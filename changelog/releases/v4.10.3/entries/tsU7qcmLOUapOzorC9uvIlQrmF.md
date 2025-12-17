---
title: "Update the submodule pointers to include periodic platform reconnects"
type: change
author: Dakostu
created: 2024-03-12T11:54:14Z
pr: 3997
---

Tenzir nodes no longer attempt reconnecting to app.tenzir.com immediately upon
failure, but rather wait before reconnecting.
