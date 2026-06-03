---
title: Google SecOps Chronicle import API
type: breaking
authors:
  - raxyte
  - codex
created: 2026-06-03T00:00:00Z
---

The `to_google_secops` operator now uses the Chronicle `logs.import`,
`events.import`, and `entities.import` APIs instead of the legacy unstructured
ingestion API.

The operator now targets a SecOps instance with `project`, `region`, and
`instance`, authenticates with Google Cloud OAuth2 credentials, and supports raw
log ingestion with `mode="log"`, UDM event ingestion with `mode="udm"`, and
entity ingestion with `mode="entity"`.
