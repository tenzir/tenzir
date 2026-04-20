---
title: New executor support for Azure Log Analytics
type: change
authors:
  - tobim
  - codex
pr: 6060
created: 2026-04-20T14:42:23.016138Z
---

The `to_azure_log_analytics` operator now works when running pipelines with the new executor:

```sh
tenzir --neo 'from {x: 1}
to_azure_log_analytics tenant_id="...", client_id="...", client_secret="...", dce="...", dcr="...", stream="..."'
```

This makes Azure Log Analytics ingestion available to pipelines that opt in to the new execution engine.
