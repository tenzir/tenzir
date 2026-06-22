---
title: Platform-managed HTTP authentications
type: feature
authors:
  - Zedoraps
  - claude
prs:
  - 6362
created: 2026-06-16T12:31:58.712788Z
---

The `auth=` option on `from_http` and `to_http` now resolves authentications
defined in the connected Tenzir Platform's authentication store as a fallback
when no matching entry exists under `tenzir.auth` in the local
`tenzir.yaml`. Use it the same way you reference a local entry:

```tql
from_http "https://example.com/api/v2/logs", auth="my-auth0"
```

Public fields like `client_id` and `token_url` arrive in plaintext; the
sensitive parts (e.g. `client_secret`, `api_key`, `token`, `password`) are
returned by the platform as references to managed secrets and resolved through
the existing secret-resolution path. As a result, rotating a referenced secret
on the platform takes effect on the next request — no node restart needed. The
local `tenzir.auth` path still takes precedence over the platform lookup.
