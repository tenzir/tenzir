---
title: Fix mTLS enforcement, TLS key passwords, and `max-backoff`
type: bugfix
authors:
  - zedoraps
  - claude
---

Several TLS options were read incorrectly and are now honored:

- `require-client-cert` (and the per-operator `tls={require_client_cert: true}`)
  is now enforced for operators that build their TLS context on the folly path
  (`accept_tcp`, `from_tcp`, `serve_tcp`, `to_tcp`, `to_http`, `to_opensearch2`,
  `from_mysql`). Previously these operators loaded neither the client CA nor the
  `fail-if-no-peer-cert` flag, so a client presenting no certificate was silently
  accepted.
- `tls.password` is now applied when loading an encrypted private key on the
  folly path. For the HTTP-server-based operators (`accept_http`,
  `accept_opensearch`, `serve_http`), an inline `tls.password` is now rejected
  with a clear diagnostic instead of being silently misinterpreted as a password
  file path.
- `tls={password: "…"}` is no longer rejected as an unknown key in the operator
  `tls` record.

Additionally, `tenzir.demand.max-backoff` is now read from the correct
configuration key. It previously read `min-backoff`, so the setting had no
effect.
