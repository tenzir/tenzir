---
title: Collect Microsoft 365 activity
type: feature
authors:
  - mavam
created: 2026-09-02T16:14:03.249324Z
---

Use the new `from_microsoft_365_activity` operator to collect unified audit
records from Microsoft 365. It handles Activity API subscriptions, bounded
backfills, continuous polling, pagination, retries, and snapshot-backed blob
checkpoints.

For example, collect general audit events as they become available:

```tql
from_microsoft_365_activity auth={
    tenant_id: secret("m365-tenant-id"),
    client_id: secret("m365-client-id"),
    client_secret: secret("m365-client-secret"),
  },
  content_types=["Audit.General"]
```

The operator emits the original audit fields together with source metadata:

```tql
{
  Id: "8f18c3d0-…",
  Operation: "UserLoggedIn",
  microsoft_365_activity: {
    tenant_id: "7f44c930-…",
    content_type: "Audit.General",
    content_id: "20260902123456789000$…",
    content_created: 2026-09-02T12:34:56Z,
  },
}
```
