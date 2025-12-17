---
title: "Fix handling of empty records in `write_parquet`"
type: bugfix
author: dominiklohmann
created: 2024-12-20T16:07:10Z
pr: 4874
---

`write_parquet` now gracefully handles nested empty records by replacing them
with nulls. The Apache Parquet format does fundamentally not support empty
nested records.
