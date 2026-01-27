---
title: "Add savers for curl connectors"
type: feature
author: mavam
created: 2024-01-16T13:37:12Z
pr: 3539
---

The `http` and `https` loaders now also have savers to send data from a pipeline
to a remote API.

The `http` and `https` connectors have a new flag `--form` to submit the request
body URL-encoded. This also changes the Content-Type header to
`application/x-www-form-urlencoded`.
