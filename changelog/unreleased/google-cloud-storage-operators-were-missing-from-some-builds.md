---
title: Google Cloud Storage operators were missing from some builds
type: bugfix
authors:
  - zedoraps
  - claude
prs:
  - 6426
created: 2026-07-08T20:22:23.188152Z
---

The gcs plugin silently disabled itself when Apache Arrow reported its GCS support flag as `TRUE` instead of `ON`, removing `from_google_cloud_storage`, `to_google_cloud_storage`, `load_gcs`, and `save_gcs` from affected builds. The plugin now checks the flag's truth value, so these operators are available again whenever Arrow ships with GCS support.
