---
title: "Fixed panic in `write_parquet`"
type: bugfix
author: dominiklohmann
created: 2025-06-20T11:44:11Z
pr: 5293
---

The `write_parquet` operator no longer panics when specifying
`compression_type="snappy"` without a `compression_level`.
