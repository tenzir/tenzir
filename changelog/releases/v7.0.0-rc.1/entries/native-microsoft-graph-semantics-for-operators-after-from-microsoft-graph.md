---
title: Native Microsoft Graph semantics for operators after from_microsoft_graph
type: change
authors:
  - mavam
created: 2026-11-11T15:39:12.181083Z
---

`from_microsoft_graph` now hands Microsoft Graph the `where`, `select`, and `head` operators after it as OData query parameters. They leave the pipeline and run in Graph, with Graph's semantics. For example, this pipeline:

```tql
from_microsoft_graph "users",
  auth={
    tenant_id: secret("graph-tenant-id"),
    client_id: secret("graph-client-id"),
    client_secret: secret("graph-client-secret"),
  }
where department == "Security"
where accountEnabled == true
select id, name=displayName, mail
head 50
```

requests a single page:

```text
/v1.0/users?$filter=(department eq 'Security') and (accountEnabled eq true)&$select=id,displayName,mail&$top=50
```

Graph may silently ignore filters that it does not support, so `where` becomes `$filter` only for the resources and properties for which Microsoft documents `$filter` support. Use `pushdown=false` to keep every operator in the pipeline with TQL semantics.
