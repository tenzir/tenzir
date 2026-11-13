---
title: Content-skipping filters and limits after from_microsoft_365_activity
type: change
authors:
  - mavam
created: 2026-11-11T16:04:34.359337Z
---

`from_microsoft_365_activity` now uses the operators after it to skip downloads. The Management Activity API cannot filter events, but every event of a content blob carries the same metadata. A `where` on `microsoft_365_activity` therefore decides which blobs the operator downloads, and `head` stops listing and downloading:

```tql
from_microsoft_365_activity auth={
    tenant_id: secret("m365-tenant-id"),
    client_id: secret("m365-client-id"),
    client_secret: secret("m365-client-secret"),
  },
  content_types=["Audit.Exchange", "Audit.SharePoint"],
  start_time=2026-06-01T00:00:00Z,
  end_time=2026-06-02T00:00:00Z
where microsoft_365_activity.content_type == "Audit.Exchange"
where microsoft_365_activity.content_created >= 2026-06-01T12:00:00Z
head 1000
```

This pipeline lists only `Audit.Exchange` content from 12:00 on, and stops after 1,000 events. The filters keep TQL semantics, so the results stay the same. Use `pushdown=false` to download every blob.
