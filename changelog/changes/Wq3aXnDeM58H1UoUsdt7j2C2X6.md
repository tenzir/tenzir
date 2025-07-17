---
title: "Fall back to string when parsing config options from environment"
type: bugfix
authors: dispanser
pr: 2305
---

Setting the environment variable `VAST_ENDPOINT` to `host:port` pair no longer
fails on startup with a parse error.
