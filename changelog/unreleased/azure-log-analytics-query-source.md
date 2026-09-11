---
title: Azure Log Analytics query source
type: feature
authors:
  - mavam
created: 2026-09-11T05:45:20.334384Z
---

The new experimental `from_azure_log_analytics` operator runs KQL queries against an Azure Log Analytics workspace and emits typed result rows. Use it to read workspace data, including tables used by Microsoft Sentinel, into a Tenzir pipeline:

```tql
from_azure_log_analytics "SecurityEvent | project TimeGenerated, Computer, EventID",
  workspace_id=secret("azure-workspace-id"),
  azure_auth={
    tenant_id: secret("azure-tenant-id"),
    client_id: secret("azure-client-id"),
    client_secret: secret("azure-client-secret"),
  },
  start=2026-01-01,
  end=2026-01-02
```

The operator supports app-only Entra authentication, optional paired time bounds, and configurable query timeouts. It validates the full response before emitting rows, preserves exact decimal values as strings, retries transient failures, and rejects service-reported partial results. It does not paginate or poll continuously; restarting reruns the query. Grant the application workspace query permissions, which are separate from ingestion permissions.
