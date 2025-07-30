---
title: "More supported types in `read_parquet"
type: feature
authors: IyeOnline
pr: 5373
---

Tenzir's does not support all types that Parquet supports. We have enabled the
`read_parquet` operator to accept more types that are convertible to supported
types. It will convert integer, floating point, and time types to the appropriate
(wider) Tenzir type. For example, if your Parquet file contains a column of type
`int32`, it will now be read in as `int64` instead of rejecting the entire file.
