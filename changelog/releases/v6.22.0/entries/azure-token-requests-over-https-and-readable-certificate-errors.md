---
title: Azure token requests over HTTPS and readable certificate errors
type: bugfix
authors:
  - mavam
created: 2026-11-05T18:55:46.223049Z
---

`from_microsoft_defender`, `from_microsoft_graph`, `from_microsoft_365_activity`, and `from_azure_log_analytics` can fetch Microsoft Entra ID access tokens over HTTPS again. Since v6.20.0, every token request failed with `certificate verify failed`, because the token endpoint was verified against the host name of the API server.

The `tls` options of these operators and the node's `tls` configuration now apply to the token endpoint as follows: `cacert`, `skip_peer_verification`, `min_version`, and `ciphers` apply, but `certfile` and `keyfile` do not, so the token endpoint never receives the client certificate. Likewise, when `from_http` follows pagination links to another host, it no longer sends the client certificate there.

HTTP clients now also name the reason when a server certificate fails verification and stop retrying:

```
error: failed to fetch Azure access token: TLS certificate verification failed for `login.microsoftonline.com`: unable to get local issuer certificate
```

Previously, the error only read `SSL routines::certificate verify failed`.

Connections to HTTPS proxies now also trust the CA bundle that the `SSL_CERT_FILE` environment variable names, like all other HTTPS connections, instead of only the system's default bundles. When an HTTP client cannot set up TLS for its server or proxy, for example because no CA bundle exists, or cannot resolve the host, operators now report an error instead of failing with `uncaught exception in pipeline`.
