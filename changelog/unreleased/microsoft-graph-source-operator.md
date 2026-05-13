---
title: Microsoft Graph source operator
type: feature
authors:
  - mavam
  - codex
created: 2026-05-12T19:40:09.318698Z
---

Tenzir now includes a Microsoft Graph source operator for one-shot reads from Microsoft Graph `v1.0` and `beta` collections with app-only Microsoft Entra authentication and OData pagination.

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
