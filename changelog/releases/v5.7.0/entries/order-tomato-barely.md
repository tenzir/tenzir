---
title: "Add an option to add extra headers to the platform request"
type: feature
author: lava
created: 2025-06-26T12:40:20Z
pr: 5287
---

The new option `tenzir.platform-extra-headers` causes the Tenzir Node to add the given extra HTTP headers when
establishing the connection to the Tenzir Platform, for example to pass additional authentication headers
when traversing proxies.

You can set this variable either via configuration file:

```yaml
tenzir:
  platform-extra-headers:
    Authentication: Bearer XXXX
    Proxy-Authentication: Bearer YYYY
```

or as environment variable: (note the double underscore before the name of the header)

```sh
TENZIR_PLATFORM_EXTRA_HEADERS__AUTHENTICATION="Bearer XXXX"
TENZIR_PLATFORM_EXTRA_HEADERS__PROXY_AUTHENTICATION="Bearer YYYY"
```

When using the environment variable version, the Tenzir Node always converts the name of the header to lowercase
and converts underscores to dashes, so a header specified as `TENZIR_PLATFORM_EXTRA_HEADERS__EXTRA_HEADER=extra`
will be sent as `extra-header: extra` in the HTTP request.
