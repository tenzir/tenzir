---
title: "Add plugin options to enable self-signed platform certificates"
type: feature
authors: lava
pr: 4918
---

The `platform` plugin now understands the `skip-peer-verification`
and `cacert` options in order to enable connections to self-hosted
platform instances with self-signed TLS certificates.
