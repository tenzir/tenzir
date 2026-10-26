---
title: Manual controls for deployment pipelines
type: feature
authors:
  - aljazerzen
created: 2026-10-26T15:24:28.756398Z
---

You can now start and gracefully stop existing deployment pipelines without
changing their branch configuration. Use the pipeline table's actions menu or
the pipeline detail button, the REST API, or the MCP tools `start_pipeline` and
`stop_pipeline`.

The platform CLI exposes the same operations:

```sh
tenzir platform deployment start-pipeline demo pipelines/ingest.tql
tenzir platform deployment stop-pipeline demo pipelines/ingest.tql
```

Start runs the deployed definition, not pending branch changes. Stop requests a
graceful shutdown without waiting for the run to drain. The Owner, Admin,
Operator, and Developer roles have the new `Deployment.startPipeline` and
`Deployment.stopPipeline` permissions by default; custom roles can grant each
permission separately.
