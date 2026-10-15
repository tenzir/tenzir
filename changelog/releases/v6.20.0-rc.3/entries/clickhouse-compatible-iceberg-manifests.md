---
title: "ClickHouse-compatible Iceberg manifests from `to_iceberg`"
type: bugfix
authors:
  - mavam
created: 2026-10-15T10:45:00.000000Z
---

The `to_iceberg` operator now writes manifest files whose `partition-spec` and
`schema-id` metadata follow the Iceberg specification. Previously, the
`partition-spec` entry contained the full partition spec object instead of
only its list of fields, and the `schema-id` entry was missing. Readers that
rely on these entries, such as ClickHouse, failed to read tables written by
`to_iceberg`.

The fix applies to manifests that `to_iceberg` writes from now on. Existing
manifests keep their previous metadata, so readers such as ClickHouse may
still fail on tables that contain them.
