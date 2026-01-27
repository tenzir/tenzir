---
title: "Add CORS preflight request handling"
type: feature
author: lava
created: 2023-02-16T10:33:46Z
pr: 2944
---

The experimental web frontend now correctly responds to CORS preflight requests.
To configure CORS behavior, the new `vast.web.cors-allowed-origin` config option
can be used.
