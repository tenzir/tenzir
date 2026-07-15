---
title: Google SecOps unstructured ingestion support
type: bugfix
authors:
  - mavam
  - codex
prs:
  - 6446
created: 2026-07-14T07:31:07.142341Z
---

The `to_google_secops` operator can once again forward raw logs without parsing
their timestamps by selecting the supported Ingestion API:

```tql
from {raw_log: "<134>1 2026-07-14T09:00:00Z host app - - - message"}
to_google_secops api="ingestion",
  log_text=raw_log,
  log_type="CUSTOM_JSON",
  private_key=secret("google-private-key"),
  client_email=secret("google-client-email"),
  customer_id=secret("google-customer-id")
```

With `api="ingestion"`, `log_entry_time` is optional so that Google SecOps can
derive the timestamp from the raw log. The `api="import"` path remains the
default for compatibility with Tenzir 6.2, and UDM events and entities continue
to use the Import API.
