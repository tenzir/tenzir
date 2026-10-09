---
title: Client certificates of TLS servers
type: bugfix
authors:
  - mavam
created: 2026-10-03T18:15:40.8897Z
---

Operators that accept TLS connections, such as `accept_http`, `accept_opensearch`, `accept_otlp`, `accept_splunk`, `accept_wef`, and `serve_http`, now verify client certificates only against `tls.client_ca`. Previously, they also trusted the CA certificates of `tls.cacert`, which defaults to the CA bundle of the system, so a client certificate from any public CA passed verification even when `tls.client_ca` named a private CA.

Without `tls.client_ca`, these operators no longer ask clients for a certificate.

A TLS server whose certificate or CA files fail to load now reports the error when the pipeline starts instead of hanging.
