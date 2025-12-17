---
title: "Fall back to string when parsing config options from environment"
type: bugfix
author: dispanser
created: 2022-05-25T11:02:16Z
pr: 2305
---

Setting the environment variable `VAST_ENDPOINT` to `host:port` pair no longer
fails on startup with a parse error.
