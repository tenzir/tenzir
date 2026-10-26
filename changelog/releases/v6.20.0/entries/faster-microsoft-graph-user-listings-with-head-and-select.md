---
title: Faster Microsoft Graph user listings with head and select
type: change
authors:
  - mavam
created: 2026-10-21T16:09:09.448974Z
---

The `from_microsoft_graph` operator now applies the `where`, `select`, and `head` operators that follow it while it pages through a resource. It stops paging and polling for delta changes as soon as `head` has enough events, so a quick look at a large tenant no longer downloads every page:

```tql
from_microsoft_graph "users",
  auth={
    tenant_id: "...",
    client_id: "...",
    client_secret: secret("graph-client-secret"),
  }
select id, displayName, mail
head 10
```

When listing `users` in the `v1.0` API, the operator also asks Microsoft Graph for less data. A `head` without a preceding `where` sets the page size with `$top`, up to the maximum of 999. A `select` that only keeps properties from the default set that Graph returns for users, such as `id`, `displayName`, and `mail`, becomes a `$select` that requests only those properties.

Filters always run locally with exact TQL semantics and never become a `$filter`. Your own `odata` options take precedence over the translated ones, and other resources, the `beta` API, and delta queries keep every request unchanged.
