---
sidebar_custom_props:
  connector:
    loader: true
    saver: true
---

# https

Loads bytes via HTTPS.

## Synopsis

```
https [-P|--skip-peer-verification] [-H|--skip-hostname-verification]
      [<method>] <url> [<item>..]
```

## Description

The `https` loader is an alias for the [`http`](http.md) connector with a
default URL scheme of `https`.

### `-P|--skip-peer-verification`

Skips certificate verification.

By default, a HTTPS connection verifies the authenticity of the peer's
certificate. During connection negotiation, the server sends a certificate
indicating its identity. We verify whether the certificate is authentic,
i.e., that you can trust that the server is who the certificate says it is.

Providing this flag disables loading of the CA certificates and verification of
the peer certificate.

### `-H|--skip-hostname-verification`

Ignores verification of the server name in the certificate.

When negotiating TLS and SSL connections, the server sends a certificate
indicating its identity. By default, that certificate must indicate that the
server is the server to which you meant to connect, or the connection fails.
That is, the server has to have the same name in the certificate as is in the
URL you operate against. We consider the server the intended one when the
*Common Name* field or a *Subject Alternate Name* field in the certificate
matches the hostname in the URL.

Providing this flag skips this check, but it makes the connection insecure.
