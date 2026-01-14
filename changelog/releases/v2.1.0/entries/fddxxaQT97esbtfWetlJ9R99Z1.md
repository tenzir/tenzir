---
title: "PRs 2334-KaanSK"
type: feature
author: KaanSK
created: 2022-06-10T10:53:24Z
pr: 2334
---

PyVAST now supports running client commands for VAST servers running in a
container environment, if no local VAST binary is available. Specify the
`container` keyword to customize this behavior. It defaults to `{"runtime":
"docker", "name": "vast"}`.
