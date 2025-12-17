---
title: "Remove the legacy metrics system"
type: feature
author: dominiklohmann
created: 2024-08-21T20:29:43Z
pr: 4381
---

The new `rebuild` metrics contain information about running partition rebuilds.

The `ingest` metrics contain information about all ingested events and their
schema. This is slightly different from the existing `import` metrics, which
track only events imported via the `import` operator, and are separate per
pipeline.
