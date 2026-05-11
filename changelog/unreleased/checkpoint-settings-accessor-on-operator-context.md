---
title: Checkpoint settings accessor on operator context
type: feature
authors:
  - jachris
prs:
  - 6151
created: 2026-05-11T10:23:02.979509Z
---

Operators can now query the surrounding pipeline's checkpoint settings via
`ctx.checkpoint_settings()` on `OpCtx`. The accessor returns an
`Option<CheckpointSettings const&>` exposing the configured `interval` and
`exactly_once` flag, or `None` when checkpointing is disabled for the
pipeline.

This lets operators adapt their behavior based on whether their work will be
durably committed at checkpoint boundaries. As a first consumer, the file
source operators (`from_file`, `from_s3`, `from_azure_blob_storage`,
`from_google_cloud_storage`) now delete processed source files immediately
when checkpointing is disabled, fixing a leak where deferred cleanup would
never run because no `post_commit` was ever invoked.
