---
title: "Fixed panic in `write_parquet`"
type: bugfix
authors: dominiklohmann
pr: 5293
---

The `write_parquet` operator no longer panics when specifying
`compression_type="snappy"` without a `compression_level`.
