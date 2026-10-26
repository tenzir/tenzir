---
title: Hostname verification for TLS client connections
type: bugfix
authors:
  - lava
created: 2026-10-26T12:00:00.000000Z
---

Operators that connect to HTTPS servers, such as `from_http`, `to_http`, `to_opensearch`, `to_splunk`, and `to_prometheus`, now enable TLS hostname validation by default: the server certificate must match the hostname or IP address in the URL. Previously, they accepted any certificate issued by a trusted CA, even one issued for a different host. The same applies to TLS connections to HTTPS proxies.

Connections with `tls.skip_peer_verification` set to `true` continue to accept any certificate.
