---
title: Complete GeoIP record enrichment
type: bugfix
authors:
  - mavam
  - codex
prs:
  - 6239
created: 2026-05-29T10:35:15.531152Z
---

GeoIP enrichment now includes all fields from MaxMind records. Previously, records containing `uint128` values could stop materializing subsequent fields, which could omit later enrichment data from affected databases.
