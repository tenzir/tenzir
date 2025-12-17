---
title: "Add plugin options to enable self-signed platform certificates"
type: feature
author: lava
created: 2025-01-27T10:39:48Z
pr: 4918
---

The `platform` plugin now understands the `skip-peer-verification`
and `cacert` options in order to enable connections to self-hosted
platform instances with self-signed TLS certificates.
