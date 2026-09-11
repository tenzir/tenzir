---
title: Microsoft Defender advanced hunting source
type: feature
authors:
  - mavam
created: 2026-09-09T12:52:42.602206Z
---

The new `from_microsoft_defender` operator runs arbitrary KQL queries against Microsoft Defender advanced hunting through Microsoft Graph and emits typed result rows. It supports app-only Entra authentication and optional paired `start` and `end` timestamps.

```tql
from_microsoft_defender "DeviceEvents | take 10",
  azure_auth={
    tenant_id: secret("AZURE_TENANT_ID"),
    client_id: secret("AZURE_CLIENT_ID"),
    client_secret: secret("AZURE_CLIENT_SECRET"),
  }
```

Grant the Microsoft Graph application permission `ThreatHunting.Read.All` with administrator consent. This finite source validates the full response before emitting rows, retries transient failures, and fails on service errors. It does not paginate or maintain a cursor; restarting reruns the query.
