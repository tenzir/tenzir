---
title: "PRs 2334-KaanSK"
type: feature
authors: KaanSK
pr: 2334
---

PyVAST now supports running client commands for VAST servers running in a
container environment, if no local VAST binary is available. Specify the
`container` keyword to customize this behavior. It defaults to `{"runtime":
"docker", "name": "vast"}`.
