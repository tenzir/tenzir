---
title: Microsoft Graph source operator
type: feature
authors:
  - mavam
  - codex
prs:
  - 6165
  - 6179
  - 6182
created: 2026-05-12T19:40:09.318698Z
---

Tenzir now includes a Microsoft Graph source operator for reads from Microsoft Graph `v1.0` and `beta` collections with app-only Microsoft Entra authentication and OData pagination.

For example, you can read Entra ID sign-in logs with client credentials and push down OData query options:

```tql
from_microsoft_graph "auditLogs/signIns",
  auth={
    tenant_id: "contoso.onmicrosoft.com",
    client_id: "00000000-0000-0000-0000-000000000000",
    client_secret: secret("ms-graph-client-secret"),
  },
  odata={
    filter: "createdDateTime ge 2026-04-24T00:00:00Z",
    select: ["id", "createdDateTime", "userPrincipalName", "status"],
    top: 1000,
  }
```

The operator emits each object from the response `value` array as a separate event and follows `@odata.nextLink` until the collection is exhausted.

The operator can also use Microsoft Graph delta queries with `delta=true`, storing the returned `@odata.deltaLink` in memory and polling it with a configurable `poll_interval`. OData query options apply to the initial delta request only, subject to Microsoft Graph's resource-specific support, and subsequent polls use the opaque delta link exactly as Microsoft Graph returned it.

It also retries throttled and transient Microsoft Graph requests, respecting `Retry-After` when present.
