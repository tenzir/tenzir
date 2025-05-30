---
title: "Support disabling TLS in `https` connector"
type: feature
authors: mavam
pr: 4248
---

The `https` connector supports the new options `--skip-peer-verification` and
`--skip-hostname-verification` to disable verification of the peer's certificate
and verification of the certificate hostname.
