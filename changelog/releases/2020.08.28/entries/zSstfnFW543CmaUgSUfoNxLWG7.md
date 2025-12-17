---
title: "Expand CAF stream slot ids to 32 bits"
type: change
author: lava
created: 2020-08-14T11:37:01Z
pr: 1020
---

We now bundle a patched version of CAF, with a changed ABI. This means that if
you're linking against the bundled CAF library, you also need to distribute that
library so that VAST can use it at runtime. The versions are API compatible so
linking against a system version of CAF is still possible and supported.
