---
title: "Include hidden pipelines in `show pipelines`"
type: change
author: dominiklohmann
created: 2024-06-19T18:12:15Z
pr: 4309
---

`show pipelines` now includes "hidden" pipelines run by the by the Tenzir
Platform or through the API. These pipelines usually run background jobs, so
they're intentionally hidden from the `/pipeline/list` API.
