---
title: Table creation on Amazon S3 Tables in to_iceberg
type: bugfix
authors:
  - zedoraps
created: 2026-11-26T11:44:47.586676Z
---

The `to_iceberg` operator now creates tables in an Amazon S3 Tables catalog. Previously, creating a table failed with the error `stage-create is a required field and cannot be null`, while appending to an existing table worked.
