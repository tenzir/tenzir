---
title: Node-level TLS configuration for operators
type: feature
authors:
  - IyeOnline
pr: 5559
created: 2025-12-17T16:33:15.093565Z
---

All operators and connectors that use TLS now support centralized node-level
configuration. Instead of passing TLS options to each operator individually,
you can configure them once in `tenzir.yaml` under `tenzir.tls`.

Arguments passed directly to the operator itself via an argument take precedence
over the configuration entry.

The following options are available:

- `enable`: Enable TLS on all operators that support it
- `skip-peer-verification`: Disable certificate verification
- `cacert`: Path to a CA certificate bundle for server verification
- `certfile`: Path to a client certificate file
- `keyfile`: Path to a client private key file
- `tls-min-version`: Minimum TLS protocol version (`"1.0"`, `"1.1"`, `"1.2"`, or `"1.3"`)
- `tls-ciphers`: OpenSSL cipher list string

The later two options have also been added as operator arguments.

For server-mode operators (`load_http server=true`, `load_tcp`), mutual TLS (mTLS)
authentication is now supported:

- `tls-client-ca`: Path to a CA certificate for validating client certificates
- `tls-require-client-cert`: Require clients to present valid certificates

These two options are also available as operator arguments.

Example configuration enforcing TLS 1.2+ with specific ciphers:

```yaml
tenzir:
  tls:
    tls-min-version: "1.2"
    tls-ciphers: "ECDHE-ECDSA-AES128-GCM-SHA256:ECDHE-RSA-AES128-GCM-SHA256"
    cacert: "/etc/ssl/certs/ca-certificates.crt"
```
