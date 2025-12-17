---
title: "Support disabling TLS in `https` connector"
type: feature
author: mavam
created: 2024-05-27T13:56:02Z
pr: 4248
---

The `https` connector supports the new options `--skip-peer-verification` and
`--skip-hostname-verification` to disable verification of the peer's certificate
and verification of the certificate hostname.
