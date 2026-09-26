---
title: Faster Parquet reads of constant and all-null columns
type: change
authors:
  - mavam
created: 2026-09-25T17:13:05.496904Z
---

`read_parquet` now skips decoding columns whose statistics show that a row group holds a single value or only nulls, and takes their values from the statistics instead. This applies to most types, such as integers, strings, timestamps, and IP addresses, also within records.

Normalized data has many such columns: in OCSF events, fields like `class_uid`, `activity_id`, or `metadata.version` often hold one value per row group, and fields like `cloud.zone` are often null throughout.

In a benchmark of OCSF network activity events, this reduced the CPU time of a full read by another 11%.
