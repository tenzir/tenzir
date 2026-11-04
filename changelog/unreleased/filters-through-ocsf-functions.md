---
title: Filters through ocsf_cast and field selection functions
type: change
authors:
  - mavam
created: 2026-11-03T15:22:05.000000Z
---

Filters on fields of the result of `ocsf_cast`, `select_matching`, and `drop_matching` now move in front of the call and read its input. Behind `this = ocsf_cast(this)`, a filter such as `where src_endpoint.hostname == "dc01"` passes the cast and the mapping before it, so that it reaches the source. Once the mapping assigns `class_uid` and `metadata.version` as constants, a comparison of a copied field with a constant becomes a plain comparison of the mapped input field, which sources such as `read_parquet` or databases evaluate, plus a local check that the value has the type that the cast requires.
